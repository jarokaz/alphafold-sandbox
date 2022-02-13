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
from re import I

from kfp.v2 import dsl
from kfp.v2.dsl import Output, Input, Artifact, Dataset


_COMPONENTS_IMAGE = os.getenv('COMPONENTS_IMAGE', 'gcr.io/jk-mlops-dev/alphafold-components')


@dsl.component(
    base_image=_COMPONENTS_IMAGE,
    output_component_file='component_template_search.yaml'
)
def template_search(
    project: str,
    region: str,
    template_dbs: list,
    mmcif_db: str,
    obsolete_db: str,
    max_template_date: str,
    reference_databases: Input[Dataset],
    sequence: Input[Dataset],
    msa: Input[Dataset],
    template_hits: Output[Dataset],
    template_features: Output[Dataset],
    cls_logging: Output[Artifact],
    machine_type:str='c2-standard-8',
    n_cpu:int=8,
    boot_disk_size:int=200,
    maxseq:int=1_000_000,
    max_template_hits:int=20, 
    ):
    """Searches for protein templates 

    This is a simple prototype using dsub to submit a Cloud Life Sciences pipeline.
    We are using CLS as KFP does not support attaching pre-populated disks or premtible VMs.
    GCSFuse does not perform well with tools like hhsearch or hhblits.

    he prototype also lacks job control. If a pipeline step fails, the CLS job can get 
    orphaned

    """
    
    import logging
    import os
    import sys

    from dsub_wrapper import run_dsub_job

    _DSUB_PROVIDER = 'google-cls-v2'
    _LOG_INTERVAL = '30s'
    _ALPHAFOLD_RUNNER_IMAGE = 'gcr.io/jk-mlops-dev/alphafold'
    _SCRIPT = '/scripts/alphafold_runners/hhsearch_runner.py'
   
    # For a prototype we are hardcoding some values. Whe productionizing
    # we can make them compile time or runtime parameters
    # E.g. CPU type is important. HHBlits requires at least SSE2 instruction set
    # Works better with AVX2. 
    # At runtime we could pass them as tool_options dictionary


    logging.basicConfig(format='%(asctime)s - %(message)s',
                      level=logging.INFO, 
                      datefmt='%d-%m-%y %H:%M:%S',
                      stream=sys.stdout)


    disk_image = reference_databases.metadata['disk_image']
    database_paths = [reference_databases.metadata[database]
                      for database in template_dbs]
    database_paths = ','.join(database_paths)
    mmcif_path = reference_databases.metadata[mmcif_db]
    obsolete_path = reference_databases.metadata[obsolete_db]

    sequence_path = sequence.uri
    msa_path = msa.uri
    msa_data_format = msa.metadata['data_format']
    template_hits.metadata['data_format'] = 'hhr'
    template_features.metadata['data_format'] = 'pkl'

    job_params = [
        '--machine-type', machine_type,
        '--boot-disk-size', str(boot_disk_size),
        '--logging', cls_logging.uri,
        '--log-interval', _LOG_INTERVAL, 
        '--image', _ALPHAFOLD_RUNNER_IMAGE,
        '--env', f'PYTHONPATH=/app/alphafold',
        '--mount', f'DB_ROOT={disk_image}',
        '--input', f'INPUT_SEQUENCE_PATH={sequence_path}',
        '--input', f'INPUT_MSA_PATH={msa_path}',
        '--output', f'OUTPUT_TEMPLATE_HITS_PATH={template_hits.uri}',
        '--output', f'OUTPUT_TEMPLATE_FEATURES_PATH={template_features.uri}',
        '--env', f'MSA_DATA_FORMAT={msa_data_format}',
        '--env', f'DB_PATHS={database_paths}',
        '--env', f'MMCIF_PATH={mmcif_path}',
        '--env', f'OBSOLETE_PATH={obsolete_path}',
        '--env', f'N_CPU={n_cpu}',
        '--env', f'MAXSEQ={maxseq}', 
        '--env', f'MAX_TEMPLATE_HITS={max_template_hits}',
        '--evn', f'MAX_TEMPLATE_DATE={max_template_date}', 
        '--script', _SCRIPT, 
    ]

    result = run_dsub_job(
        provider=_DSUB_PROVIDER,
        project=project,
        regions=region,
        params=job_params,
    ) 

    

    
    



    
