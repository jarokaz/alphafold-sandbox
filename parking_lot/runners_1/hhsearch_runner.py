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

import logging
import os
import numpy as np
import pathlib
import shutil
import sys

from typing import Any, Mapping, MutableMapping, Optional, Sequence, Union

from alphafold.common import residue_constants
from alphafold.data import msa_identifiers
from alphafold.data import parsers
from alphafold.data import templates
from alphafold.data.tools import hhsearch 


_OUTPUT_FILE_NAME = 'output.hhr'
MSA_PATH = os.environ['MSA_PATH']
OUTPUT_DIR = os.environ['OUTPUT_DIR']
DATABASE_PATHS = os.environ['DATABASE_PATHS']
HHBLITS_BINARY_PATH = shutil.which('hhsearch')
MAXSEQ = int(os.getenv('MAXSEQ'))


def run_hhsearch(
    input_msa_path: str,
    database_paths: Sequence[str],
    maxseq: int,
    output_dir: str): 
    """Runs hhblits and saves results to a file."""

    runner = hhsearch.HHSearch(
        binary_path=HHBLITS_BINARY_PATH,
        databases=database_paths,
        maxseq=maxseq,
    )

    with open(input_msa_path) as f:
        input_msa_str = f.read()

    file_type = pathlib.Path(input_msa_path).suffix
    if  file_type == '.sto':
        msa_for_templates = parsers.deduplicate_stockholm_msa(input_msa_str)
        msa_for_templates = parsers.remove_empty_columns_from_stockholm_msa(msa_for_templates)
        msa_for_templates = parsers.convert_stockholm_to_a3m(msa_for_templates)
        print('sto')
    elif file_type == '.a3m':
        # TBD - research what kind of preprocessing required for a3m - if any 
        print('a3m')
    else:
        raise ValueError(
          f'File type not supported by HHSearch: {file_type}.')

    template_hits = runner.query(msa_for_templates)
    
    template_out_path = os.path.join(output_dir, _OUTPUT_FILE_NAME)
    with open(template_out_path, 'w') as f:
        f.write(template_hits)
    logging.info(f"Saved results to {template_out_path}")


if __name__=='__main__':
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        level=logging.INFO, 
                        datefmt='%d-%m-%y %H:%M:%S',
                        stream=sys.stdout)
    
    run_hhsearch(
        input_msa_path=MSA_PATH,
        database_paths=DATABASE_PATHS.split(','),
        maxseq=MAXSEQ,
        output_dir=OUTPUT_DIR
    )