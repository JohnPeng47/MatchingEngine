from lambda_runtime.main.data_model.order import Order
from botocore.exceptions import ClientError
from lambda_runtime.main.routes.create_order_operation import match_order

import json

def updateOrderOperation(event):
    # we assume that the client sends the entire order, as well as the old price
    # change to support querying using another localsecondaryindex on user_id
    old_price = event["body"]["old_price"]
    order_id = event["body"]["order"]["order_id"]
    ddb_conn = event["client"]
    
    # TODO: if not updating price then use update_item instead
    try:
        item = ddb_conn.get_item(
            Key = {
                "Pk" : {
                    "S" : order_id
                },
                "Sk" : {
                    "N" : str(old_price)
                }
            },
            TableName="LimitOrderBookTest",
            ConsistentRead=True
        )
        if not item.get("Item", None):
            return {
                "statusCode": 403,
                "body": "Item does not exist, primary key (order_id, old_price) does not reference an existing order"
            }

        print("item" , item["Item"])
        order = Order(event["body"]["order"], mode="update", original=item["Item"])
        # Since price is the sort key, need to delete old item before we can update
        ddb_conn.delete_item(
            Key = {
                "Pk" : {
                    "S" : order.order_id
                },
                "Sk" : {
                    "N" : str(old_price)
                }
            },
            TableName="LimitOrderBookTest"
        )
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            return {
                "statusCode": 403,
                "body": "Cannot update partially filled order"
            }

    res = match_order(ddb_conn, order)
    return res