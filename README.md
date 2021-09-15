###Installation Steps:
1. In the project root directory, run `python build.py`. This will generate zip archives of each subdirectory under `lambda_runtime` for deploying to lambda
2. Change directory to `sam`. The SAM (Serverless Application Model: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html) is required to deploy the Cloudformation infrastructure file
3. Run `sam local start-api --template-file infra.yaml` to deploy a local version of the API hosted using Docker at localhost:3000
4. Use the postman requests to interact with the local lambda endpoints

TODO:
- make explicit references to DDB Tables depend on a config file instead of hard coded value
