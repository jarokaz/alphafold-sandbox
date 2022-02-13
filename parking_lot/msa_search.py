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


import os
from kfp.v2 import dsl
from kfp.v2.dsl import Output, Input, Artifact, Dataset


_COMPONENTS_IMAGE = os.getenv('COMPONENTS_IMAGE', 'gcr.io/jk-mlops-dev/alphafold-components')


@dsl.component(
    base_image=_COMPONENTS_IMAGE,
    output_component_file='component_msa_search.yaml'
)
def msa_search(
    project: str,
    region: str,
    msa_dbs: list,
    reference_databases: Input[Dataset],
    sequence: Input[Dataset],
    msa: Output[Dataset],
    cls_logging: Output[Artifact] 
    ):
    """Searches sequence databases using the specified tool.

    This is a simple prototype using dsub to submit a Cloud Life Sciences pipeline.
    We are using CLS as KFP does not support attaching pre-populated disks or premtible VMs.
    GCSFuse does not perform well with tools like hhsearch or hhblits.

    The prototype also lacks job control. If a pipeline step fails, the CLS job can get 
    orphaned

    """
    
    import logging
    import os
    import sys

    from dsub_wrapper import run_dsub_job

    _UNIREF90 = 'uniref90'
    _MGNIFY = 'mgnify'
    _BFD = 'bfd'
    _UNICLUST30 = 'uniclust30'
    _UNIPROT = 'uniprot'

    _DSUB_PROVIDER = 'google-cls-v2'
    _LOG_INTERVAL = '30s'
    _ALPHAFOLD_RUNNER_IMAGE = 'gcr.io/jk-mlops-dev/alphafold'

    _DEFAULT_FILE_PREFIX = 'datafile'

    # For a prototype we are hardcoding some values. Whe productionizing
    # we can make them compile time or runtime parameters
    # E.g. CPU type is important. HHBlits requires at least SSE2 instruction set
    # Works better with AVX2. 
    # At runtime we could pass them as tool_options dictionary
    logging.basicConfig(format='%(asctime)s - %(message)s',
                      level=logging.INFO, 
                      datefmt='%d-%m-%y %H:%M:%S',
                      stream=sys.stdout)

    _TOOL_TO_SETTINGS_MAPPING = {
       'jackhmmer': {
           'MACHINE_TYPE': 'n1-standard-8',
           'BOOT_DISK_SIZE': '200',
           'OUTPUT_DATA_FORMAT': 'sto',
           'N_CPU': '8',
           'MAXSEQ': '10_000',
           'SCRIPT': '/scripts/alphafold_runners/jackhmmer_runner.py' 
       },
       'hhblits': {
           'MACHINE_TYPE': 'c2-standard-8',
           'BOOT_DISK_SIZE': '200',
           'OUTPUT_DATA_FORMAT': 'a3m',
           'N_CPU': '8',
           'MAXSEQ': '1_000_000',
           'SCRIPT': '/scripts/alphafold_runners/hhblits_runner.py' 
       },
    }
     
    # This is a temporary crude solution to map a the list of databases to search
    # to a search tool. In the prototype we assume that the provided databases list 
    # can be searched with a single tool
    _DATABASE_TO_TOOL_MAPPING = {
        _UNIREF90: 'jackhmmer',
        _MGNIFY: 'jackhmmer',
        _BFD: 'hhblits',
        _UNICLUST30: 'hhblits', 
        _UNIPROT: None, # to be determined
    }
    
    tools = [_DATABASE_TO_TOOL_MAPPING[db] for db in msa_dbs
              if _DATABASE_TO_TOOL_MAPPING[db]]
    tools = list(set(tools))

    if (not tools) or (len(tools) > 1):
        raise RuntimeError(f'The database list {msa_dbs} not supported')
    db_tool = tools[0]

    disk_image = reference_databases.metadata['disk_image']
    database_paths = [reference_databases.metadata[database]
                      for database in msa_dbs]
    database_paths = ','.join(database_paths)

    output_data_format = _TOOL_TO_SETTINGS_MAPPING[db_tool]['OUTPUT_DATA_FORMAT']
    msa.metadata['data_format'] = output_data_format
    output_path = msa.uri
    input_path = sequence.uri
    
    job_params = [
        '--machine-type', _TOOL_TO_SETTINGS_MAPPING[db_tool]['MACHINE_TYPE'],
        '--boot-disk-size', _TOOL_TO_SETTINGS_MAPPING[db_tool]['BOOT_DISK_SIZE'],
        '--logging', cls_logging.uri,
        '--log-interval', _LOG_INTERVAL, 
        '--image', _ALPHAFOLD_RUNNER_IMAGE,
        '--env', f'PYTHONPATH=/app/alphafold',
        '--mount', f'DB_ROOT={disk_image}',
        '--input', f'INPUT_PATH={input_path}',
        '--output', f'OUTPUT_PATH={output_path}',
        '--env', f'DB_TOOL={db_tool}',
        '--env', f'DB_PATHS={database_paths}',
        '--env', f'N_CPU={_TOOL_TO_SETTINGS_MAPPING[db_tool]["N_CPU"]}',
        '--env', f'MAXSEQ={_TOOL_TO_SETTINGS_MAPPING[db_tool]["MAXSEQ"]}', 
        '--script', _TOOL_TO_SETTINGS_MAPPING[db_tool]['SCRIPT'] 
    ]

    result = run_dsub_job(
        provider=_DSUB_PROVIDER,
        project=project,
        regions=region,
        params=job_params,
    ) 

    

    
    



    
