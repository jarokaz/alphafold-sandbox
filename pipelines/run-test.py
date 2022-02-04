import os
import json
import sys
import time

from typing import Dict, Union, Optional, List


from absl import app
from absl import flags
from absl import logging

import google.cloud.aiplatform as aip

from datetime import datetime
import kfp

from kfp.v2 import components
from kfp.v2 import dsl
from kfp.v2 import compiler
from kfp.v2.dsl import component

FLAGS = flags.FLAGS
IMAGE_NAME = 'gcr.io/jk-mlops-dev/hhsearch-test'
PACKAGE_PATH = "alphafold.json"

REGION = 'us-central1'

logging.set_verbosity(logging.INFO)

flags.DEFINE_string(
    'fasta_path', None, 'A path to FASTA file')
flags.DEFINE_string('data_dir', None, 'Path to directory of supporting data.')
flags.DEFINE_string('output_dir', None, 'Path to a directory that will '
                    'store the results.')
flags.DEFINE_string(
    'msa_path', '/output/msas/uniref90_hits.sto', 'A path to msa in Stockholm format')
flags.DEFINE_integer(
    'max_sto_sequences', 501, 'A maximum number of sequences to use for template search'
)
flags.DEFINE_string('project_id', None, 'Project ID ')

flags.DEFINE_string('staging_bucket', None, 'Staging bucket')



def main(argv):

    aip.init(project=FLAGS.project_id, 
             staging_bucket=f'{FLAGS.staging_bucket}/alphafold_sandbox')



    component_spec = f"""
    name: Run hhsearch
    description: Runs hhsearch

    implementation:
      container:
        image: {IMAGE_NAME}
        args: [
            '--data_dir={FLAGS.data_dir}',
            '--fasta_path={FLAGS.fasta_path}',
            '--msa_path={FLAGS.msa_path}',
            '--max_sto_sequences={FLAGS.max_sto_sequences}',
            '--output_dir={FLAGS.output_dir}'
        ]
    """

    run_hhsearch_op = kfp.components.load_component_from_text(component_spec)

    @dsl.pipeline(name="alphafold-test")
    def pipeline():
        run_hhsearch_step = run_hhsearch_op()

        run_hhsearch_step.set_cpu_limit('24')
        run_hhsearch_step.set_memory_limit('80G')
        run_hhsearch_step.set_env_variable(
            name='NVIDIA_VISIBLE_DEVICES', value='all')
        run_hhsearch_step.set_env_variable(
            name='TF_FORCE_UNIFIED_MEMORY', value='1')
        run_hhsearch_step.set_env_variable(
            name='XLA_PYTHON_CLIENT_MEM_FRACTION', value='4.0')

    compiler.Compiler().compile(
        pipeline_func=pipeline, package_path=PACKAGE_PATH
    )

    DISPLAY_NAME = "alphafold_test_" + datetime.now().strftime("%Y%m%d%H%M%S")
    PIPELINE_ROOT = f'{FLAGS.staging_bucket}/alphafold/pipeline_root'
    VERTEX_SA = f'vertex-sa@{FLAGS.project_id}.iam.gserviceaccount.com'

    job = aip.PipelineJob(
        display_name=DISPLAY_NAME,
        template_path=PACKAGE_PATH,
        pipeline_root=PIPELINE_ROOT,
    )

    job.run(service_account=VERTEX_SA)

if __name__=='__main__':
    flags.mark_flags_as_required([
        'fasta_path',
        'data_dir',
        'output_dir'
    ])
    app.run(main)