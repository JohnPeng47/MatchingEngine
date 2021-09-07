import glob
import os
import subprocess
import boto3
import argparse
from os import path
from typing import Optional
#os.chdir("/mnt/c/Users/pengjohn/Documents/tools/fantasy")

# we have to keep track of order of resource creation, because some resources might depend on another resource
def zip_lambda(lambda_dir)->Optional[bytearray]:
    if lambda_dir == None:
        lambda_dir = "*"
    directories = glob.glob(path.join("lambda_runtime", lambda_dir))
    zipped_files = []
    for d in directories:
        # zip_dir = os.path.abspath(d)
        print("Zipping payload in directory {}".format(d))
        if os.path.isdir(d):
            files = glob.glob(d + "/*")
            build_zip = "build/{}.zip".format(d[d.index("/"):])
            # * in front of list unpacks its arguments?
            print(f'ARGS: {["zip", "-r", "build.zip", *files]}')
            err, out = subprocess.Popen([f"zip", "-r", "{}".format(build_zip), *files]).communicate()
            if len(directories) == 1:
                with open(build_zip,'rb') as zipped:
                    return zipped.read()
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("tobuild", help="Which lambda folder to build", default=None, nargs="?")
    # parser.add_argument("--bucket", help="Specifies bucket to push the lambda")
    parser.add_argument("--update-function", help="Whether to update the lambda function or not")
    parser.add_argument("--function-name", help="Name of lambda")
    args = parser.parse_args()
    tobuild = args.tobuild
    update_function = args.update_function
    function_name = args.function_name

    zipped_file = zip_lambda(tobuild) 

    try:
        if update_function:
            s3_client = boto3.client("s3")
            lambda_client = boto3.client("lambda")
            
            # s = Pipeline("./cloudformation")
            # zip the lambdas
            res = lambda_client.update_function_code(FunctionName=function_name, ZipFile=zipped_file)
    except Exception:
        print("When update_function is specified, must specify only one tobuild directory")