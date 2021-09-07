import os
import boto3
import sys
import json

from lambda_runtime.main.routes.create_order_operation import createOrderOperation
from lambda_runtime.main.routes.delete_order_operation import deleteOrderOperation
from lambda_runtime.main.routes.update_order_operation import updateOrderOperation

# client initialization outside of handler function to take advantage of lambda container warm start
db_client = boto3.client('dynamodb')

PATH_TO_FUNCTION = {
    "create": createOrderOperation,
    "update": updateOrderOperation,
    "delete": deleteOrderOperation
}

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # no error catching here, assume that APIGW has completed input validation
    event["body"] = json.loads(event["body"])
    event["client"] = db_client
    # event["order"] = json.loads(event["order"])
    operation = event["body"]["operation"]
    print("calling operation {}".format(operation))
    
    func = PATH_TO_FUNCTION.get(operation, None)
    # if authz_layer(userRole, path, userEmail): TODO: implement authorization layer here
    results = call_function(event, func)
    statusCode = 500
    error_message = f"Path {operation} does not exist"

    return results

def call_function(event, func):
    print(f"Calling {func}")
    res = func(event)
    return res

