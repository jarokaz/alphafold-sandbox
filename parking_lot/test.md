docker run -it --rm --entrypoint /bin/bash \
-v /home/jupyter/alphafold-inference-pipeline:/src \
-v /mnt/disks/alphafold-datasets:/data \
gcr.io/jk-mlops-dev/alphafold-components

export PYTHONPATH=/src/alphafold_components

pytest -s dsub_wrapper_test.py::test_jackhmmer_job

pytest -s dsub_wrapper_test.py::test_hhblits_job

pytest -s dsub_wrapper_test.py::test_hhsearch_job 



###

python dsub_wrapper.py \
--project jk-mlops-dev \
--regions us-central1 \
--machine-type n1-standard-8 \
--boot-disk-size 100 \
--log-interval 30s \
--provider local \
--input INPUT_PATH=gs://jk-dsub-staging/fasta/T1050.fasta \
--output OUTPUT_PATH=gs://jk-dsub-staging/output/jackhmmer/data_file \
--env "N_CPU=4 MAX_STO_SEQUENCES=10"
