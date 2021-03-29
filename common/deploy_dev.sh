#!/bin/bash

#Zipping
echo Start zipping...
cd $1
zip -g ../common/zip/lambda_importer_$1.zip importer_$1.py
cd ../
cd common
zip -g ./zip/lambda_importer_$1.zip utils.py
cd ../
cd env/lib/python3.7/site-packages
zip -r9 ../../../../common/zip/lambda_importer_$1.zip .
cd ../../../../
echo Finished zipping!

#S3
echo Start S3 upload!
aws s3 cp ./common/zip/lambda_importer_$1.zip s3://tropsy-lambda-bucket/
echo Finished S3 upload!

# Lambda
echo Start Lambda update!
aws lambda update-function-code \
  --function-name importer_$1 \
  --s3-bucket tropsy-lambda-bucket \
  --s3-key lambda_importer_$1.zip

PUBLISHED_VERSION=$(aws lambda publish-version --function-name importer_$1 | jq -r .Version)

aws lambda update-alias \
  --function-name importer_$1 \
  --name DEV \
  --function-version $PUBLISHED_VERSION

echo Finished Lambda update!
