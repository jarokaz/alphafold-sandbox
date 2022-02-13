
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
"""Launcher client to launch jobs for various job types."""

import argparse
import logging
import os
import sys

from pprint import pprint


#from . import cls_runner
import cls_runner


def _make_parent_dirs_and_return_path(file_path: str):
  #os.makedirs(os.path.dirname(file_path), exist_ok=True)
  return file_path


def _parse_args(args):
  """Parse command line arguments."""
  parser = argparse.ArgumentParser(
      prog='Vertex Pipelines service launcher', description='')
  parser.add_argument(
      '--type', dest='type', type=str, required=True, default=argparse.SUPPRESS)
  parser.add_argument(
      '--project',
      dest='project',
      type=str,
      required=True,
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--location',
      dest='location',
      type=str,
      required=True,
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--payload',
      dest='payload',
      type=str,
      required=True,
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--gcp_resources',
      dest='gcp_resources',
      type=_make_parent_dirs_and_return_path,
      required=True,
      default=argparse.SUPPRESS)
  parsed_args, _ = parser.parse_known_args(args)
  # Parse the conditionally required arguments
  parser.add_argument(
      '--executor_input',
      dest='executor_input',
      type=str,
      # executor_input is only needed for components that emit output artifacts.
      required=(parsed_args.type in {
          'UploadModel', 'CreateEndpoint', 'BatchPredictionJob',
          'BigqueryQueryJob', 'BigqueryCreateModelJob',
          'BigqueryPredictModelJob', 'BigQueryEvaluateModelJob'
      }),
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--output_info',
      dest='output_info',
      type=str,
      # output_info is only needed for ExportModel component.
      required=(parsed_args.type == 'ExportModel'),
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--job_configuration_query_override',
      dest='job_configuration_query_override',
      type=str,
      required=(parsed_args.type in {
          'BigqueryQueryJob', 'BigqueryCreateModelJob',
          'BigqueryPredictModelJob', 'BigQueryEvaluateModelJob'
      }),
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--model_name',
      dest='model_name',
      type=str,
      required=(parsed_args.type in {
          'BigqueryPredictModelJob', 'BigqueryExportModelJob',
          'BigQueryEvaluateModelJob'
      }),
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--model_destination_path',
      dest='model_destination_path',
      type=str,
      required=(parsed_args.type == 'BigqueryExportModelJob'),
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--exported_model_path',
      dest='exported_model_path',
      type=str,
      required=(parsed_args.type == 'BigqueryExportModelJob'),
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--table_name',
      dest='table_name',
      type=str,
      # table_name is only needed for BigQuery tvf model job component.
      required=(parsed_args.type
                in {'BigqueryPredictModelJob', 'BigQueryEvaluateModelJob'}),
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--query_statement',
      dest='query_statement',
      type=str,
      # query_statement is only needed for BigQuery predict model job component.
      required=(parsed_args.type
                in {'BigqueryPredictModelJob', 'BigQueryEvaluateModelJob'}),
      default=argparse.SUPPRESS)
  parser.add_argument(
      '--threshold',
      dest='threshold',
      type=float,
      # threshold is only needed for BigQuery tvf model job component.
      required=(parsed_args.type
                in {'BigqueryPredictModelJob', 'BigQueryEvaluateModelJob'}),
      default=argparse.SUPPRESS)
  parsed_args, _ = parser.parse_known_args(args)
  return vars(parsed_args)

def run_cls_pipeline(
    type,
    project,
    location,
    payload,
    gcp_resources,
):
    """Create and poll custom job status till it reaches a final state.
    This follows the typical launching logic:
    1. Read if the custom job already exists in gcp_resources
        - If already exists, jump to step 3 and poll the job status. This happens
        if the launcher container experienced unexpected termination, such as
        preemption
    2. Deserialize the payload into the job spec and create the custom job
    3. Poll the custom job status every _POLLING_INTERVAL_IN_SECONDS seconds
        - If the custom job is succeeded, return succeeded
        - If the custom job is cancelled/paused, it's an unexpected scenario so
        return failed
        - If the custom job is running, continue polling the status
    Also retry on ConnectionError up to
    job_remote_runner._CONNECTION_ERROR_RETRY_LIMIT times during the poll.
    """

    print(type)
    print(project)
    print(location)
    print(payload)
    print(gcp_resources)




    pipeline = {
        "actions": [
            {
                "container_name": 'alphafold-inference',
                "image_uri": 'gcr.io/jk-mlops-dev/alphafold-inference',
                "commands": [
                        'echo', 'hello' 
                ],
                #"entrypoint": '/bin/bash',
            },
        ],
        "resources": {
            "regions": ["us-central1"],
            "virtual_machine": {
                "machine_type": "n1-standard-4",
                'boot_disk_size_gb': 500,
            },
        }
    }
    

    pipeline_runner = cls_runner.PipelineRunner(project, location)

    lro = pipeline_runner.run_pipeline(pipeline)
    print(lro)

   # credentials = GoogleCredentials.get_application_default()

   # service = discovery.build('lifesciences', 'v2beta', credentials=credentials)

   # # The project and location that this request should be executed against.
   # parent = f'projects/{project}/locations/{location}'  

   # run_pipeline_request_body = {
   #     # TODO: Add desired entries to the request body.
   # }

   # request = service.projects().locations().pipelines().run(parent=parent, body=run_pipeline_request_body)
   # response = request.execute()

    #pprint(response)

    #remote_runner = job_remote_runner.JobRemoteRunner(type, project, location,
    #                                                  gcp_resources)

    ## Create custom job if it does not exist
    #job_name = remote_runner.check_if_job_exists()
    #if job_name is None:
    #    job_name = remote_runner.create_job(create_custom_job_with_client,
    #                                        payload)

    ## Poll custom job status until "JobState.JOB_STATE_SUCCEEDED"
    #remote_runner.poll_job(get_custom_job_with_client, job_name)


def main(argv):
  """Main entry.
  Eexpected input args are as follows:
    Project - Required. The project of which the resource will be launched.
    Region - Required. The region of which the resource will be launched.
    Type - Required. GCP launcher is a single container. This Enum will
        specify which resource to be launched.
    Request payload - Required. The full serialized json of the resource spec.
        Note this can contain the Pipeline Placeholders.
    gcp_resources placeholder output for returning job_id.
  Args:
    argv: A list of system arguments.
  """
  parsed_args = _parse_args(argv)
  job_type = parsed_args['type']

  run_cls_pipeline(**parsed_args)


if __name__ == '__main__':
  main(sys.argv[1:])