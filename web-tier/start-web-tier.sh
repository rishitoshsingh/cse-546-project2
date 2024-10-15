#!/bin/bash
source /home/ubuntu/miniconda3/etc/profile.d/conda.sh
conda activate cloud-aws
gunicorn -b 0.0.0.0:8000 app:app