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

"""A script for searching sequence databases using hhblits."""

import logging
import os
import numpy as np
import shutil
import sys

from typing import Any, Mapping, MutableMapping, Optional, Sequence, Union

from alphafold.common import residue_constants
from alphafold.data import msa_identifiers
from alphafold.data import parsers
from alphafold.data import templates
from alphafold.data.tools import hhblits


_OUTPUT_FILE_NAME = 'output_hhblits.a3m'
_DEFAULT_MSA_FORMAT = 'a3m'

FASTA_PATH = os.environ['FASTA_PATH']
OUTPUT_DIR = os.environ['OUTPUT_DIR']
DATABASE_PATHS = os.environ['DATABASE_PATHS']
N_CPU = int(os.environ['N_CPU'])
HHBLITS_BINARY_PATH = shutil.which('hhblits')


def run_hhblits(
    input_fasta_path: str,
    database_paths: Sequence[str],
    n_cpu: int,
    output_dir: str): 
    """Runs hhblits and saves results to a file."""

    runner = hhblits.HHBlits(
        binary_path=HHBLITS_BINARY_PATH,
        databases=database_paths,
        n_cpu=n_cpu
    )

    with open(input_fasta_path) as f:
        input_fasta_str = f.read()
    input_seqs, input_descs = parsers.parse_fasta(input_fasta_str)
    if len(input_seqs) != 1:
      raise ValueError(
          f'More than one input sequence found in {input_fasta_path}.')

    msa_out_path = os.path.join(output_dir, _OUTPUT_FILE_NAME)
    result = runner.query(input_fasta_path)[0]
    with open(msa_out_path, 'w') as f:
        f.write(result[_DEFAULT_MSA_FORMAT])
    logging.info(f"Saved results to {msa_out_path}")


if __name__=='__main__':
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        level=logging.INFO, 
                        datefmt='%d-%m-%y %H:%M:%S',
                        stream=sys.stdout)
    
    run_hhblits(
        input_fasta_path=FASTA_PATH,
        database_paths=DATABASE_PATHS.split(','),
        n_cpu=N_CPU,
        output_dir=OUTPUT_DIR
    )