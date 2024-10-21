#!/bin/bash
source /home/ubuntu/miniconda3/etc/profile.d/conda.sh
conda activate cloud-aws-async
# uvicorn app:app --host 0.0.0.0 --port 8000
# gunicorn app:app --bind 0.0.0.0:8000 --worker-class uvicorn.workers.UvicornWorker
gunicorn app:app --workers 10 --timeout 600