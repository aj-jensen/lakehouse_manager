#!/bin/bash
# 0th arg ($0) is the filename itself, not used
# 1th argument ($1) is the name of the S3 bucket where the file should go to, e.g.:
# s3://acme-inc-lakehouse-manager-bucket
# 2nd arg ($2) is the aws credentials profile to use, should have perms for the S3 bucket in question
cd ../
python setup.py bdist_wheel
cd dist
aws s3 cp dremio_actions-1.0-py3-none-any.whl $1/dremio_actions-1.0-py3-none-any.whl --profile $2
rm -rf ../build
rm -rf ../dist
rm -rf ../dremio_actions.egg-info