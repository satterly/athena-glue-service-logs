# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at

# http://www.apache.org/licenses/LICENSE-2.0

# or in the "license" file accompanying this file. This file is distributed 
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
# express or implied. See the License for the specific language governing 
# permissions and limitations under the License.

"""DateTimePartioner is an implementation of a Partioner where data is structured in S3 in a Y/m/d/hh format"""
import time
from datetime import datetime, timedelta

from athena_glue_service_logs.partitioners.base_partitioner import BasePartitioner
from athena_glue_service_logs.utils import S3Reader


class DateTimePartitioner(BasePartitioner):
    """DatePartioner is an implementation of a Partioner where data is structured in S3 in a Y/m/d/hh format"""
    MAX_RECENT_HOURS = 72

    def build_partitions_from_s3(self):
        partition_values = []
        s3_reader = S3Reader(self.s3_location)

        # Get first date and add to data catalog
        # Then add all partitions after that date
        if self.hive_compatible:
            key_names = [key['Name'] for key in self.partition_keys()]
            first_partition = s3_reader.get_first_hivecompatible_datetime_in_prefix(key_names)
        else:
            first_partition = s3_reader.get_first_datetime_in_prefix()
        partition_values.append(first_partition)
        partition_values += self._get_datetime_values_since_initial_time(first_partition)

        return partition_values

    def partition_keys(self):
        return [
            {"Name": "year", "Type": "string"},
            {"Name": "month", "Type": "string"},
            {"Name": "day", "Type": "string"},
            {"Name": "hour", "Type": "string"},
        ]

    def find_recent_partitions(self, existing_partitions):
        partitions_to_add = []

        # Now check to see if, in each region, that a partition day exists for today.
        # If it does not, backfill up to today.
        now = datetime.utcfromtimestamp(time.time())
        hour_diff = 0

        # Only go back MAX_RECENT_HOURS hours for now and only if S3 objects actually exist...
        for _ in range(self.MAX_RECENT_HOURS):
            new_hour = now + timedelta(hours=hour_diff)
            new_hour_tuple = new_hour.strftime('%Y-%m-%d-%H').split('-')
            if not existing_partitions or existing_partitions[-1] != new_hour_tuple:
                if S3Reader(self.build_partitioned_path(new_hour_tuple)).does_have_objects():
                    partitions_to_add.append(new_hour_tuple)
            else:
                break
            hour_diff -= 1

        return partitions_to_add
