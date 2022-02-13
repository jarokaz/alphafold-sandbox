#!/usr/bin/env python 
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
import time
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
from alphafold.data.tools import jackhmmer

#from runner_utils import run_msa_tool


_OUTPUT_FILE_NAME = 'output.sto'
_DEFAULT_MSA_FORMAT = 'sto'
FLAGS = flags.FLAGS

#logging.set_verbosity(logging.INFO)

FASTA_PATH = os.getenv('FASTA_PATH', 'somewhere')
N_CPU = os.getenv('N_CPU', '100')
DB = os.getenv('DB', '/db')



def run_jackhmmer(
    input_fasta_path: str,
    database_path: str,
    n_cpu: int,
    max_sto_sequences: int,
    output_dir: str): 
    """Runs jackhmeer and saves results to a file."""

    runner = jackhmmer.Jackhmmer(
        binary_path=FLAGS.jackhmmer_binary_path,
        database_path=database_path,
        n_cpu=n_cpu,
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
        max_sto_sequences=max_sto_sequences,
        use_precomputed_msas=False
    )

    return result


def run_test():

    print(FASTA_PATH)
    print(N_CPU)
    print(DB)
    print(os.listdir(DB))
    print('Sleeping for a minute or so')
    time.sleep(60)

if __name__=='__main__':
    run_test()