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

"""A script for searching sequence databases."""

import os
from re import I
import numpy as np
import shutil


from absl import logging
from absl import flags
from absl import app
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Union


from alphafold.common import residue_constants
from alphafold.data import msa_identifiers
from alphafold.data import parsers
from alphafold.data import templates
from alphafold.data.tools import hmmsearch 

from runner_utils import run_msa_tool


_OUTPUT_FILE_NAME = 'output.a3m'
_DEFAULT_MSA_FORMAT = 'a3m'
FLAGS = flags.FLAGS

logging.set_verbosity(logging.INFO)

flags.DEFINE_string(
    'fasta_path', None, 'A path to FASTA file')

flags.DEFINE_list(
    'database_paths', None, 'Paths to a list of sequence databases to search')
flags.DEFINE_string('output_dir', None, 'Path to a directory that will '
                    'store the results.')
flags.DEFINE_integer(
    'n_cpu', 4, 'The number of CPUs to give Jackhmmer'
)
flags.DEFINE_integer(
    'max_sto_sequences', 501, 'A maximum number of sequences to use for template search'
)
flags.DEFINE_string('hmmsearch_binary_path', shutil.which('hmmsearch'),
                    'Path to the hmmsearch  executable.')
flags.DEFINE_string('hmmbuild_binary_path', shutil.which('hmmbuild'),
                    'Path to the hmmbuild executable.')

def run_hhblits(
    input_fasta_path: str,
    database_paths: Sequence[str],
    output_dir: str): 
    """Runs hhblits and saves results to a file."""

    runner = hmmsearch.Hmmsearch(
        binary_path=FLAGS.hhblits_binary_path,
        hmmbuild_binary_path=FLAGS.hmmbuild_binary_path
        databases=database_paths,
    )

    with open(input_fasta_path) as f:
        input_fasta_str = f.read()
    input_seqs, input_descs = parsers.parse_fasta(input_fasta_str)
    if len(input_seqs) != 1:
      raise ValueError(
          f'More than one input sequence found in {input_fasta_path}.')

    msa_out_path = os.path.join(output_dir, _OUTPUT_FILE_NAME)
    result = run_msa_tool(
        msa_runner=runner,
        input_fasta_path=input_fasta_path,
        msa_out_path=msa_out_path,
        msa_format=_DEFAULT_MSA_FORMAT,
        use_precomputed_msas=False
    )

    return result


def _main(argv):

    result = run_hhblits(
        input_fasta_path=FLAGS.fasta_path,
        database_paths=FLAGS.database_paths,
        n_cpu=FLAGS.n_cpu,
        output_dir=FLAGS.output_dir
    ) 


if __name__=='__main__':
    flags.mark_flags_as_required([
        'fasta_path',
        'database_paths',
        'output_dir'
    ])
    app.run(_main)