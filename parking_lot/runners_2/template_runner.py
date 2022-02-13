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

import logging
import os
import numpy as np
import pathlib
import shutil
import sys
import time

from typing import Any, Mapping, MutableMapping, Optional, Sequence, Union

from alphafold.common import residue_constants
from alphafold.data import msa_identifiers
from alphafold.data import parsers
from alphafold.data import templates
from alphafold.data.tools import hhsearch 


INPUT_PATH = os.environ['INPUT_PATH']
OUTPUT_PATH = os.environ['OUTPUT_PATH']
DATABASES_ROOT = os.environ['DATABASES_ROOT']
DATABASE_PATHS = os.environ['DATABASE_PATHS']
HHBLITS_BINARY_PATH = shutil.which('hhsearch')
MAXSEQ = int(os.getenv('MAXSEQ', '1_000_000'))
TEMPLATE_TOOL = os.environ['TEMPLATE_TOOL']


def run_hhsearch(
    input_path: str,
    database_paths: Sequence[str],
    maxseq: int,
    output_path: str): 
    """Runs hhblits and saves results to a file."""

    template_format = pathlib.Path(input_path).suffix[1:]
    if template_format != 'hhr':
        raise ValueError('hhsearch does not support generating files in {output_file_type} format') 

    runner = hhsearch.HHSearch(
        binary_path=HHBLITS_BINARY_PATH,
        databases=database_paths,
        maxseq=maxseq,
    )

    with open(input_path) as f:
        input_msa_str = f.read()

    msa_format = pathlib.Path(input_path).suffix[1:]
    if  msa_format == 'sto':
        msa_for_templates = parsers.deduplicate_stockholm_msa(input_msa_str)
        msa_for_templates = parsers.remove_empty_columns_from_stockholm_msa(msa_for_templates)
        msa_for_templates = parsers.convert_stockholm_to_a3m(msa_for_templates)
        print('sto')
    elif msa_format == 'a3m':
        # TBD - research what kind of preprocessing required for a3m - if any 
        print('a3m')
    else:
        raise ValueError(
          f'File format not supported by HHSearch: {msa_format}.')

    template_hits = runner.query(msa_for_templates)

    with open(output_path, 'w') as f:
        f.write(template_hits)
    logging.info(f"Saved results to {output_path}")


if __name__=='__main__':
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        level=logging.INFO, 
                        datefmt='%d-%m-%y %H:%M:%S',
                        stream=sys.stdout)

    database_paths = [
            os.path.join(DATABASES_ROOT, database_path) 
            for database_path in DATABASE_PATHS.split(',')]

    if TEMPLATE_TOOL == 'hhsearch':
        database_path = DATABASE_PATHS.split(',')[0]
        run_hhsearch(
            input_msa_path=MSA_PATH,
            database_paths=database_paths,
            maxseq=MAXSEQ,
            output_dir=OUTPUT_DIR
        )
    else:
      raise ValueError(
          f'Unsupported tool {TEMPLATE_TOOL}.') 