import os
import json
import sys
import time

from typing import Dict, Union, Optional, List


from absl import app
from absl import flags
from absl import logging

FLAGS = flags.FLAGS
IMAGE_NAME = 'gcr.io/jk-mlops-dev/hhsearch-test'

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



def main(argv):
    component_spec = f"""
    name: Run hhsearch
    description: Runs hhsearch

    implementation:
    container:
        image: {IMAGE_NAME}
        args: [
            '--data_dir={FLAGS.data_dir}',
            '--fasta_paths={FLAGS.fasta_path}',
            '--msa_path={FLAGS.msa_path}',
            '--max_sto_sequences={FLAGS.max_sto_sequences}',
            '--output_dir={FLAGS.output_dir}'
        ]
    """

    print(component_spec)

if __name__=='__main__':
    flags.mark_flags_as_required([
        'fasta_path',
        'data_dir',
        'output_dir'
    ])
    app.run(main)