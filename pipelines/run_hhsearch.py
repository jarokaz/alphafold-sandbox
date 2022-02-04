import os
import json
from re import A
import sys
import time
import shutil

from typing import Dict, Union, Optional, List, Mapping, Any, MutableMapping, Sequence


from absl import app
from absl import flags
from absl import logging

from alphafold.common import residue_constants
from alphafold.data import msa_identifiers
from alphafold.data import parsers
from alphafold.data import templates
from alphafold.data.tools import hhblits
from alphafold.data.tools import hhsearch
from alphafold.data.tools import hmmsearch
from alphafold.data.tools import jackhmmer
import numpy as np

MAX_TEMPLATE_HITS = 20
FLAGS = flags.FLAGS

logging.set_verbosity(logging.INFO)

flags.DEFINE_string(
    'fasta_path', '/fasta/T1031.fasta', 'A path to FASTA file')
flags.DEFINE_string(
    'data_dir', '/data', 'Paths to template databases')
flags.DEFINE_string('output_dir', '/output', 'Path to a directory that will '
                    'store the results.')
flags.DEFINE_string(
    'msa_path', '/output/msas/uniref90_hits.sto', 'A path to msa in Stockholm format')
flags.DEFINE_integer(
    'max_sto_sequences', 501, 'A maximum number of sequences to use for template search'
)

flags.DEFINE_string('hhblits_binary_path', shutil.which('hhblits'),
                    'Path to the HHblits executable.')
flags.DEFINE_string('hhsearch_binary_path', shutil.which('hhsearch'),
                    'Path to the HHsearch executable.')
flags.DEFINE_string('hmmsearch_binary_path', shutil.which('hmmsearch'),
                    'Path to the hmmsearch executable.')
flags.DEFINE_string('hmmbuild_binary_path', shutil.which('hmmbuild'),
                    'Path to the hmmbuild executable.')
flags.DEFINE_string('kalign_binary_path', shutil.which('kalign'),
                    'Path to the Kalign executable.')

flags.DEFINE_enum('db_preset', 'full_dbs',
                  ['full_dbs', 'reduced_dbs'],
                  'Choose preset MSA database configuration - '
                  'smaller genetic database config (reduced_dbs) or '
                  'full genetic database config  (full_dbs)')

flags.DEFINE_boolean('use_precomputed_msas', False, 'Whether to read MSAs that '
                     'have been written to disk instead of running the MSA '
                     'tools. The MSA files are looked up in the output '
                     'directory, so it must stay the same between multiple '
                     'runs that are to reuse the MSAs. WARNING: This will not '
                     'check if the sequence, database or configuration have '
                     'changed.')

flags.DEFINE_string('max_template_date', '2020-05-14', 'Maximum template release date '
                    'to consider. Important if folding historical test sets.')


def load_msa(msa_path, msa_format, max_sto_sequences):
    logging.info('Reading MSA from file %s', msa_path)
    if msa_format == 'sto' and max_sto_sequences is not None:
      precomputed_msa = parsers.truncate_stockholm_msa(
          msa_path, max_sto_sequences)
      result = {'sto': precomputed_msa}
    else:
      with open(msa_path, 'r') as f:
        result = {msa_format: f.read()} 
    
    return result

def run_template_search(database_paths: List[str], 
                        input_sequence,
                        msa_for_templates,
                        msa_format):

    template_searcher = hhsearch.HHSearch(
            binary_path=FLAGS.hhsearch_binary_path,
            databases=database_paths)

    if msa_format != 'sto':
        raise ValueError(f'Unsupported input format: {msa_format}')
    
    if template_searcher.input_format == 'a3m':
        msa_for_templates = parsers.convert_stockholm_to_a3m(msa_for_templates)
        
    templates_results = template_searcher.query(msa_for_templates)

    templates_hits = template_searcher.get_template_hits(
        output_string=templates_results, input_sequence=input_sequence)

    return templates_results, templates_hits, template_searcher.output_format


def main(argv):

    
    # PDB path
     
    pdb_database_path = os.path.join(
        FLAGS.data_dir, 'pdb70', 'pdb70'
    )
    # Path to the PDB seqres database for use by hmmsearch.
    pdb_seqres_database_path = os.path.join(
        FLAGS.data_dir, 'pdb_seqres', 'pdb_seqres.txt')

    # Path to a directory with template mmCIF structures, each named <pdb_id>.cif.
    template_mmcif_dir = os.path.join(FLAGS.data_dir, 'pdb_mmcif', 'mmcif_files')

    # Path to a file mapping obsolete PDB IDs to their replacements.
    obsolete_pdbs_path = os.path.join(FLAGS.data_dir, 'pdb_mmcif', 'obsolete.dat')


    input_fasta_path = FLAGS.fasta_path

    with open(input_fasta_path) as f:
      input_fasta_str = f.read()
    input_seqs, input_descs = parsers.parse_fasta(input_fasta_str)
    if len(input_seqs) != 1:
      raise ValueError(
          f'More than one input sequence found in {input_fasta_path}.')
    input_sequence = input_seqs[0]

    msa_path = FLAGS.msa_path
    # Assume that MSA is in a Stockholm format
    msa_for_templates = load_msa(msa_path, 'sto', FLAGS.max_sto_sequences)['sto']
    msa_for_templates = parsers.deduplicate_stockholm_msa(msa_for_templates)
    msa_for_templates = parsers.remove_empty_columns_from_stockholm_msa(
            msa_for_templates)

    pdb_templates_results, pdb_templates_hits, output_format = run_template_search(
            database_paths=[pdb_database_path], 
            msa_for_templates=msa_for_templates,
            input_sequence=input_sequence,
            msa_format='sto') 


    templates_output_dir = os.path.join(FLAGS.output_dir, 'output_templates')
    os.makedirs(templates_output_dir, exist_ok=True)

    hits_out_path = os.path.join(
        templates_output_dir, f'pdb_hits.{output_format}')
    with open(hits_out_path, 'w') as f:
        f.write(pdb_templates_results) 
 
    return

    template_featurizer = templates.HhsearchHitFeaturizer(
            mmcif_dir=template_mmcif_dir,
            max_template_date=FLAGS.max_template_date,
            max_hits=MAX_TEMPLATE_HITS,
            kalign_binary_path=FLAGS.kalign_binary_path,
            release_dates_path=None,
            obsolete_pdbs_path=obsolete_pdbs_path)

    templates_result = template_featurizer.get_templates(
        query_sequence=input_sequence,
        hits=pdb_templates_hits)

    print(templates_result)

if __name__=='__main__':
    flags.mark_flags_as_required([
        #'fasta_paths',
        #'data_dir',
        #'output_dir'
    ])
    app.run(main)