from lambda_runtime.main.data_model.order import Order
from botocore.exceptions import ClientError

import json

def deleteOrderOperation(event):
    order_price = event["body"]["order_price"]
    order_id = event["body"]["order_id"]
    ddb_conn = event["client"]

    try:
        ddb_conn.delete_item(
            Key = {
                "Pk" : {
                    "S" : order_id
                },
                "Sk" : {
                    "N" : str(order_price)
                }
            },
            TableName="LimitOrderBook",
            ConditionExpression="filled = :filled",
            ExpressionAttributeValues={
                ":filled" : {
                    "BOOL" : False
                }
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            body = "Cannot delete partially filled order"

    body = "Order successfully deleted"
    return {
        "statusCode": 200,
        "body": body
    }
