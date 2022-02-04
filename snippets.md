docker run -it --rm --gpus all \
--entrypoint /bin/bash \
gcr.io/jk-mlops-dev/alphafold \
ls


python3 docker/run_docker.py \
  --fasta_paths=/home/jupyter/fasta/T1031.fasta \
  --max_template_date=2020-05-14 \
  --data_dir=$DATASETS


python run-test.py \
--data_dir /gcs/jk-alphafold-datasets-archive/nov-2021 \
--fasta_path /gcs/jk-alphafold-datasets-archive/nov-2021/fasta/T1031.fasta \
--msa_path /gcs/jk-alphafold-datasets-archive/msas/uniref90_hits.sto \
--output_dir /gcs/jk-alphafold-datasets-archive/output

