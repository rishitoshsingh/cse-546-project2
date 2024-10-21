#!/bin/bash
source /home/ubuntu/miniconda3/etc/profile.d/conda.sh
conda activate cloud-aws-async
gunicorn app:app --workers 10 --timeout 600