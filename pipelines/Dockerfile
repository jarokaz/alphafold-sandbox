FROM gcr.io/jk-mlops-dev/alphafold

WORKDIR /tests

ADD run_hhsearch.py .

ENV PYTHONPATH=/app/alphafold

RUN echo $'#!/bin/bash\n\
ldconfig\n\
python /tests/run_hhsearch.py "$@"' > /tests/run_alphafold.sh \
  && chmod +x /tests/run_alphafold.sh
ENTRYPOINT ["/tests/run_alphafold.sh"]
