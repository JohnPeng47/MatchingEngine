import sys
from os import path
from boto3 import client as aws_client
from botocore.exceptions import ClientError
import json

sys.path.append(path.dirname(path.abspath("/mnt/c/Users/pengjohn/Documents/wtf_sam/lambda_runtime")))
from lambda_runtime.main.data_model.order import Order

ddb_client = aws_client("dynamodb")

def populate_table():
    asks = json.loads(open("asks.json").read())
    ddb_asks = map(lambda ask: Order(ask), asks)
    for ask in ddb_asks:
        ddb_client.put_item(TableName="LimitOrderBook",
            Item=ask.to_ddb_item())

    bids = json.loads(open("bids.json").read())
    ddb_bids = map(lambda bid: Order(bid), bids)
    for bid in ddb_bids:
        ddb_client.put_item(TableName="LimitOrderBook",
            Item=bid.to_ddb_item())

# returns best matching orders for the order type. Parameter is used to "paginate" the
def get_best_matching_orders(order, i):
    if order.order_type == "SELL":
        res = ddb_client.query(
            Limit = 20 + i  * 20, # limit the number of orders to
            ExpressionAttributeValues = {
                ":price" : {
                    "N" : str(order.price)
                },
                ":stockNameAndType" : {
                    "S" : order.get_matching_pk()
                }
            },
            IndexName="BestMatchingOffers",
            TableName="LimitOrderBook",
            KeyConditionExpression=f"Gsi1Pk = :stockNameAndType AND Sk {'>=' if order.order_type =='SELL' else '<='} :price")
        # if no matching orders found, add full order to limit book
        if not res["Items"]:
            ddb_client.put_item(
                TableName="LimitOrderBook",
                Item=order.to_ddb_item()
            )

        best_matching = map(lambda order: Order(order), res["Items"])
        # sort first by price reversed, then by timestamp. This nested sort in python will maintain
        # order of the prices first
        best_matching = sorted(sorted(best_matching, key=lambda x: x.order_time), key=lambda x: x.price, reverse=True if order.order_type =='SELL' else False)

        # on consecutive calls of best_matching, just return the 20 latest
        return best_matching if i == 0 else best_matching[20:]

def fill_order(best_matching, order):
    filled = order.units
    finished_iteration = False
    # Design Decision: update orders on partial fills for each loop iteration or commit a single transactions containing
    # all the fills at the end of the loop? Choosing partial fills because or else big orders may be hard to fill.
    for bid in best_matching:
        filled_orders = filled
        filled = filled - bid.units if filled - bid.units > 0 else 0
        bid.units = bid.units - filled_orders if bid.units - filled_orders > 0 else 0
        if bid.units == 0:
            # conditionExpressions checks that the item we are looking at is the most recent version
            # prevents race condition
            res = ddb_client.delete_item(
                Key = {
                    "Pk" : {
                        "S" : bid.order_id
                    },
                    "Sk" : {
                        "N" : str(bid.price)
                    }
                },
                TableName="LimitOrderBook",
                ConditionExpression="order_time = :order_time",
                ExpressionAttributeValues={
                    ":order_time" : {
                        "N" : bid.order_time
                    }
                }
            )
            print("deleted: {}".format(res))

        elif bid.units > 0:
            # update item from partial fill
            res = ddb_client.put_item(
                Item = bid.to_ddb_item(),
                TableName="LimitOrderBook",
                # conditionExpressions checks that the item we are looking at is the most recent version
                # prevents race condition
                ConditionExpression="order_time = :order_time",
                ExpressionAttributeValues={
                    ":order_time" : {
                        "N" : bid.order_time
                    }
                }
            )
        if filled == 0:
            break
    order.units = order.units - filled
    if order.units > 0:
        ddb_client.put_item(
            Item = order.to_ddb_item(),
            TableName="LimitOrderBook"
        )

def delete_table():
    items = ddb_client.scan(
        TableName="LimitOrderBook",
        Select="ALL_ATTRIBUTES"
    )["Items"]
    for item in items:
        order_id, order_price = item["Pk"]["S"], item["Sk"]["N"]
        ddb_client.delete_item(
            Key = {
                "Pk" : {
                    "S" : order_id
                },
                "Sk" : {
                    "N" : str(order_price)
                }
            },
            TableName="LimitOrderBook"
        )

def scan_table():
    items = ddb_client.scan(
        TableName="LimitOrderBook",
        Select="ALL_ATTRIBUTES"
    )["Items"]
    for item in items:
        print(item)


populate_table()
# delete_table()
scan_table()

# order = {
#     "user_id": "user1",
#     "stock_symbol": "AAPL",
#     "order_type" : "SELL",
#     "units": 40,
#     "price": 102.55
# }

# items = get_best_matching_orders(Order(order))
# for item in items:
#     print(item)
