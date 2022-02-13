./run_dsub_job.sh \
--project jk-mlops-dev \
--region us-central1 \
--input-path /home/jupyter/fasta/T1050.fasta \
--machine-type n1-standar-8 \
--boot-disk-size 100 \
--alphafold-database-image "https://www.googleapis.com/compute/v1/projects/jk-mlops-dev/global/images/alphafold-datasets-jan-2022 3000" \
--output-path /home/jupyter/output/testing/jackhmmer/output.sto \
--output-path-uri gs://folder1/folder1/output.so \
--output-artifact /home/jupyter/output/artifact \
--output-artifact-uri gs://folder1/folder1/artifact 


./alphafold_launcher.sh \
--project jk-mlops-dev \
--region us-central1 \
--input INPUT_PATH=gs://jk-dsub-staging/fasta/T1050.fasta \
--output OUTPUT_PATH=gs://jk-dsub-staging/testing/jackhmmer/output.sto \
--machine-type n1-standar-8 \
--boot-disk-size 100 \
--kfp-output-artifact gs://jk-dsub-staging/artifacts \
--fsfsf


./run_dsub.sh \
--project jk-mlops-dev \
--regions us-central1 \
--machine-type n1-standar-8 \
--boot-disk-size 100 \
--image gcr.io/jk-mlops-dev/alphafold \
--script ./alphafold_runners/db_search_runner.py \
--input INPUT_PATH=gs://jk-dsub-staging/fasta/T1050.fasta \
--output OUTPUT_PATH=gs://jk-dsub-staging/testing/jackhmmer/output.sto \
--env PYTHONPATH=/app/alphafold \
--env DATABASE_PATHS=uniref90/uniref90.fasta \
--env MSA_TOOL=jackhmmer \
--env N_CPU=4 \
--env MAX_STO_SEQUENCES=10_000 \
--mount DATABASE_ROOT="https://www.googleapis.com/compute/v1/projects/jk-mlops-dev/global/images/alphafold-datasets-jan-2022 3000"


./run_dsub.sh \
--command 'sleep 30s' \
--logging gs://jk-dsub-staging/logging \
--project jk-mlops-dev 


docker run -it --rm gcr.io/jk-mlops-dev/alphafold-components \
--project jk-mlops-dev \
--logging /tmp/logging \
--command 'sleep 1m'