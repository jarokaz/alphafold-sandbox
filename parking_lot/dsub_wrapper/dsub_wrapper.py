# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A Python wrapper around dsub."""


import logging
import os
import subprocess 
import shutil

from typing import List

_DSUB_BINARY_PATH = shutil.which('dsub')

class DsubJob(object):

    def __init__(self,
                 project: str,
                 region: str,
                 image: str,
                 logging: str,
                 machine_type: str,
                 boot_disk_size: int=100,
                 log_interval: str="1m",
                 binary_path: str=_DSUB_BINARY_PATH,
                 provider: str='google-cls-v2'):

        self.binary_path=binary_path
        self.project=project
        self.region=region
        self.image=image
        self.logging=logging
        self.provider=provider 
        self.boot_disk_size=boot_disk_size
        self.log_interval=log_interval
        self.machine_type=machine_type
        
        self.base_cmd = [
            self.binary_path,
            '--image', self.image,
            '--project', self.project,
            '--regions', self.region,
            '--logging', self.logging,
            '--log-interval', self.log_interval,
            '--machine-type', self.machine_type,
            '--boot-disk-size', str(self.boot_disk_size),
            '--provider', self.provider 
        ]


    def _convert_to_parameter_list(self,
                                   input_dict: dict,
                                   param_name: str):
        param_list = [(param_name, f'{key}={value}') for key, value in input_dict.items()] 
        param_list = [element for sublist in param_list for element in sublist] 

        return param_list

    def run_job(self, 
                script: str,
                inputs: dict,
                outputs: dict,
                env_vars: dict,
                disk_mounts: dict,
                wait: bool=True)-> str:

        inputs = self._convert_to_parameter_list(inputs, '--input')
        outputs = self._convert_to_parameter_list(outputs, '--output')
        env_vars = self._convert_to_parameter_list(env_vars, '--env')
        disk_mounts = self._convert_to_parameter_list(disk_mounts, '--mount')
        script = ['--script', script]

        dsub_cmd = self.base_cmd + script + inputs + outputs + env_vars + disk_mounts
        if wait:
            dsub_cmd.append('--wait')

        logging.info(f'Executing: {dsub_cmd}')
        
        result = subprocess.run(
            dsub_cmd,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE
        )
  
        return result

    def check_job_status(self, job_id: str):
        pass

    def retrieve_logs(self, job_id: str):
        pass






