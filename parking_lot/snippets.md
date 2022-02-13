

docker run -it --rm \
-v /home/jupyter/alphafold-inference-pipeline:/src \
-v /mnt/disks/alphafold-datasets:/data \
-v /home/jupyter/alphafold-inference-pipeline/fasta:/fasta \
gcr.io/jk-mlops-dev/alphafold-inference



python msa_search.py \
--type MSASearch \
--project jk-mlops-dev \
--location us-central1 \
--payload '{}' \
--gcp_resources '{}'


python get_msas.py \
--