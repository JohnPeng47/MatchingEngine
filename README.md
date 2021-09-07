###Installation Steps:
1. In the project root directory, run `python build.py`. This will generate zip archives of each subdirectory under `lambda_runtime` for deploying to lambda
2. Change directory to `sam`. The SAM (Serverless Application Model: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html) is required to deploy the Cloudformation infrastructure file
3. Run `sam deploy --template-file infra.yaml --guided` to deploy


