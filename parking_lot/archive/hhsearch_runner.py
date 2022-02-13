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
import pathlib


from absl import logging
from absl import flags
from absl import app
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Union


from alphafold.common import residue_constants
from alphafold.data import msa_identifiers
from alphafold.data import parsers
from alphafold.data import templates
from alphafold.data.tools import hhsearch 

from runner_utils import run_msa_tool


_OUTPUT_FILE_NAME = 'output.hhr'
FLAGS = flags.FLAGS

logging.set_verbosity(logging.INFO)

flags.DEFINE_string(
    'msa_path', None, 'A path to an msa file in sto or a3m format. The file must have an extension')

flags.DEFINE_list(
    'database_paths', None, 'Paths to a list of sequence databases to search')
flags.DEFINE_string('output_dir', None, 'Path to a directory that will '
                    'store the results.')

flags.DEFINE_integer(
    'maxseq', 1_000_000, 'The maximum number of rows in an input alignment'
)
flags.DEFINE_string('hhsearch_binary_path', shutil.which('hhsearch'),
                    'Path to the HHsearch executable.')

def run_hhsearch(
    input_msa_path: str,
    database_paths: Sequence[str],
    maxseq: int,
    output_dir: str): 
    """Runs hhblits and saves results to a file."""

    runner = hhsearch.HHSearch(
        binary_path=FLAGS.hhsearch_binary_path,
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

    with open(os.path.join(output_dir, _OUTPUT_FILE_NAME), 'w') as f:
        f.write(template_hits)

    return template_hits


def _main(argv):

    result = run_hhsearch(
        input_msa_path=FLAGS.msa_path,
        database_paths=FLAGS.database_paths,
        maxseq=FLAGS.maxseq,
        output_dir=FLAGS.output_dir
    ) 


if __name__=='__main__':
    flags.mark_flags_as_required([
        'msa_path',
        'database_paths',
        'output_dir'
    ])
    app.run(_main)