from lambda_runtime.main.data_model.order import Order
# TODO: would need to strip out all imported type annotations (ie. reference to List and Tuple)
# Dont need this for deployment, only good for IDE annotations, strip out in the build script
from typing import List, Tuple
from botocore.exceptions import ClientError

from lambda_runtime.main.routes.log_transaction import log_transaction
import json

transactions = []
# Design decision this passing event
def createOrderOperation(event):
    # returns best matching orders for the order type. Parameter is used to "paginate" the 
    new_order = Order(event["body"]["order"])
    ddb_conn = event["client"]
    res = attempt_match_order(ddb_conn, new_order)
    return res

def attempt_match_order(ddb_conn, new_order):
    i = 0
    best_matching_orders = get_best_matching_orders(ddb_conn, new_order)
    # no matching orders,add to the limit order table
    if not best_matching_orders:
        # TODO: error check if write success
        # Design Decision: commit one big transaction at the end with total units modifed for new_order
        # or make incremental transactions on each fill_order loop iteration?
        # Going with: transaction on each loop iteration, because committing one big transaction at the end have a bigger chance
        # of failing due to conditionExpression ie. that item has been updated since the get_best_matching_order call. Commit updates to orders 
        # incrementally to avoid this
        res = ddb_conn.put_item(
            TableName="LimitOrderBook",
            Item = new_order.to_ddb_item()
        )
        return {
            "statusCode": 200,
            "body":  "No matching orders found, adding to the limit order table"
        }
 
    # ConditionalCheckFailedException here means that the current timestamp for best_matching_order is newer than
    # the timestamp read from the get_best_matching_orders operation, indicating that we need to requery the item from DDB
    #
    # Notes: Try to fill order conditional on the timestamp of our current item be the same as the one retrieved just now from the DB
    retry = False
    new_order_unit_count_updated = 0
    while i < len(best_matching_orders) - 1 or retry:
        if not retry:
            i += 1
            matching_order = best_matching_orders[i]
        else:
            # get a fresher version of the order if retry flag is set
            # Also should prolly wrap someof these DDB calls in util function
            # TODO: handle db exception
            retry_matching_order = ddb_conn.get_item(
                Key = {
                    "Pk" : {
                        "S" : best_matching_orders[i].order_id
                    },
                    "Sk" : {
                        "N" : str(best_matching_orders[i].price)
                    }
                },
                TableName="LimitOrderBook",
                ConsistentRead=True # probably doesn't matter here
            )["Item"]
            matching_order = retry_matching_order

        matching_order, new_order = fill_order(matching_order, new_order)
        try:
            matching_order, new_order = match_order(ddb_conn, matching_order, new_order)
            if new_order_unit_count_updated != 0:
                new_order_unit_count_updated = new_order.units
            # successful update no need to retry again
            if new_order.units == 0:
                return {
                    "statusCode": 200,
                    "body":  "Order fully filled"
                }
            retry = False
        except ClientError as e:  
            if e.response['Error']['Code']=='ConditionalCheckFailedException':  
                retry = True
                continue
    
    # if we reach here, then new_order.units > 0
    # TODO: check for write error
    # new_order_unit_count_updated is used on the off chance that the loop only has one iteration, so we can skip the following update step
    # since it would reflect the most recent update to new_order in match_order
    if new_order_unit_count_updated != new_order.units:
        res = ddb_conn.put_item(
            TableName="LimitOrderBook",
            Item = new_order.to_ddb_item()
        )

    print("Partial filled: {}".format(new_order.to_ddb_item()))
    return {
        "statusCode": 200,
        "body":  "Order partially filled"
    }

def get_best_matching_orders(ddb_conn, order: Order)->List[Order]:
    # Strongly consistent reads not supported for global secondary indices, not that it matters since
    # the values are going to be stale anyways after we iterate through them to fill orders
    res = ddb_conn.query(
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
    
    # if no matching orders found, add order to limit book
    if len(res["Items"]) == 0:
        ddb_conn.put_item(
            TableName="LimitOrderBook",
            Item=order.to_ddb_item()
        )
        return []
    
    best_matching_orders = map(lambda order: Order(order), res["Items"])
    # sort first by price reversed (descending), then by timestamp
    best_matching_orders = sorted(sorted(best_matching_orders, key=lambda x: x.order_time), key=lambda x: x.price, reverse=True if order.order_type =='SELL' else False)
    return best_matching_orders

# Note: attempts to update db entry for the filled order
def match_order(ddb_conn, best_matching, new_order):
    transact_items = []
    new_order_unit_count_updated = 0
    if best_matching.units == 0:
        best_matching_transaction = {
            'Delete': {
                'TableName': 'LimitOrderBook',
                'Key': {
                    "Pk" : {
                        "S" : best_matching.order_id
                    },  
                    "Sk" : {
                        "N" : str(best_matching.price)
                    }
                },
                'ConditionExpression': "order_time = :order_time",
                'ExpressionAttributeValues' : {
                    ":order_time" : {
                        "N" : str(best_matching.order_time)
                    }
                }
            }
        }
    else:
        best_matching_transaction = {
                'Put': {
                'TableName': 'LimitOrderBook',
                'Item': best_matching.to_ddb_item(),
                'ConditionExpression': "order_time = :order_time",
                'ExpressionAttributeValues' : {
                    ":order_time" : {
                        "N" : str(best_matching.order_time)
                    }
                }

            }
        }

    transact_items.append(best_matching_transaction)
    # only need to update once for every new_order to prevent PUT/DELETES
    if not new_order.filled:
        new_order_unit_count_updated = new_order.units
        new_order_transaction = {
            'Put': {
                'TableName': 'LimitOrderBook',
                'Item': new_order.to_ddb_item()
            }
        }
        transact_items.append(new_order_transaction)
    
    for transact in transact_items:
        print("Transact item: ", transact)

    # TODO: error check if write success
    try:
        res = ddb_conn.transact_write_items(TransactItems = transact_items)
    except Exception as e:  
        raise e

    return best_matching, new_order

def fill_order(best_matching: Order, order: Order)->Tuple[Order,Order]:
    filled_orders = order.units
    order.units = order.units - best_matching.units if order.units - best_matching.units >= 0 else 0 # might be neater with a math floor function
    best_matching.units = best_matching.units - filled_orders if best_matching.units - filled_orders >= 0 else 0

    return best_matching, order