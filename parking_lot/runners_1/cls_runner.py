
# Copyright 2021 DeepMind Technologies Limited
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
"""Common module for launching Life Sciences pipelines"""

import json
import logging
import os
import sys
import time

from pprint import pprint


from google.api_core.operation import Operation
from google.cloud.lifesciences_v2beta.services.workflows_service_v2_beta import WorkflowsServiceV2BetaClient
from google.cloud.lifesciences_v2beta.types import RunPipelineRequest
from google.cloud.lifesciences_v2beta.types import Pipeline

from typing import Any, Callable, Mapping, Optional, Sequence, Union


_POLLING_INTERVAL_IN_SECONDS = 11
_LRO_ERROR_RETRY_DELAY_IN_SECONDS = 1
_CONNECTION_ERROR_RETRY_LIMIT = 5


class PipelineRunner():
    """Class encapsulating CLS pipeline submission and control."""

    def __init__(self, project: str, location: str, gcp_resources=None):
        """Initlizes a job client and other common attributes."""
        self.project = project
        self.location = location
        self.parent = f'projects/{project}/locations/{location}'
        self.client = WorkflowsServiceV2BetaClient()
        self.last_logged_event_index = 0


    def _validate_pub_sub_topic(pub_sub_topic: str):
        """Validates pub_sub topic."""
        #TBD
        return pub_sub_topic

    def _validate_labels(labels: dict):
        """Validates labels."""
        #TBD
        return labels

    def run_pipeline(self, pipeline: Union[Pipeline, dict], labels: dict=None, pub_sub_topic: str=None) -> Operation:
        """Creates a pipeline run."""
        
        request = RunPipelineRequest() 
        request.parent = self.parent
        request.pipeline = pipeline
        if pub_sub_topic:
            request.pub_sub_topic = self._validate_pub_sub_topic(pub_sub_topic)
        if labels:
            request.labels =  self._validate_labels(labels)

        lro = self.client.run_pipeline(request)

        self._wait_for_pipeline_run(lro)

        return lro.metadata


    def _wait_for_pipeline_run(self, lro: Operation):
        """Poll the pipeline run status and waits for completion."""
        
        event_count = 0
        while True:
            
            # lro can throw a TypeError exception when transitioning states
            try: 
                status = lro.done()
                newest_event_index = len(lro.metadata.events)
                for i in range(newest_event_index - self.last_logged_event_index):
                    print(lro.metadata.events[i].description)
                self.last_logged_event_index = newest_event_index 
                if status:
                    break
            except TypeError:
                time.sleep(_LRO_ERROR_RETRY_DELAY_IN_SECONDS)
                continue
            time.sleep(_POLLING_INTERVAL_IN_SECONDS)

            # TBD. Add retry and cancelation for stuck jobs
