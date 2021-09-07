from lambda_runtime.main.data_model.order import Order
# TODO: would need to strip out all imported type annotations (ie. reference to List and Tuple)
# Dont need this for deployment, only good for IDE annotations, strip out in the build script
from typing import List, Tuple

import json

# Design decision this passing event
def createOrderOperation(event):
    # returns best matching orders for the order type. Parameter is used to "paginate" the 
    new_order = Order(event["body"]["order"])
    ddb_conn = event["client"]
    res = match_order(ddb_conn, new_order)
    return res

# attempts to match the order and makes the relevant db updates
def match_order(ddb_conn, new_order):
    best_matching_orders = get_best_matching_orders(ddb_conn, new_order)
    # no matching orders,add to the limit order table
    if not best_matching_orders:
        # TODO: error check if write success
        res = ddb_conn.put_item(
            TableName="LimitOrderBookTest",
            Item = new_order.to_ddb_item()
        )
        return {
            "statusCode": 200,
            "body":  "No matching orders found, adding to the limit order table"
        }

    for best_matching in best_matching_orders:
        best_matching, new_order = fill_order(best_matching, new_order)
        # conditionExpressions checks that the item we are looking at is the most recent version
        # prevents race condition
        # Design Decision: commit one big transaction at the end with total units modifed for new_order
        # or make incremental transactions on each fill_order loop iteration?
        # Going with: transaction on each loop iteration, because committing one big transaction at the end have a bigger chance
        # of failing due to conditionExpression ie. that item has been updated since the get_best_matching_order call. Commit incrementally
        # to avoid this
        transact_items = []
        new_order_unit_count_updated = 0
        if best_matching.units == 0:
            best_matching_transaction = {
                'Delete': {
                    'TableName': 'LimitOrderBookTest',
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
            # maybe should stick this in the outer loop, looks kinda weird repeated here, but safer I think, just in case we can have 
            # best_matching.units = 0 (although ideally this would be caught by input validation)
            if not new_order.filled:
                new_order.filled = True
        else:
            best_matching_transaction = {
                  'Put': {
                    'TableName': 'LimitOrderBookTest',
                    'Item': best_matching.to_ddb_item()
                }
            }
            if not new_order.filled:
                new_order.filled = True
                
        # only need to update once for every new_order to prevent PUT/DELETES
        transact_items.append(best_matching_transaction)
        if not new_order.filled:
            new_order_unit_count_updated = new_order.units
            new_order_transaction = {
                'Put': {
                    'TableName': 'LimitOrderBookTest',
                    'Item': new_order.to_ddb_item()
                }
            }
            transact_items.append(new_order_transaction)

        for transact in transact_items:
            print("Transact item: ", transact)

        # TODO: error check if write success
        res = ddb_conn.transact_write_items(TransactItems = transact_items)

        if new_order.units == 0:
            break

    # Potentially an extra DB write here 
    if new_order.units > 0:
        # TODO: check for write error
        # new_order_unit_count_updated is used on the off chance that the loop only has one iteration, so we can skip the following update step
        # since it would reflect the most recent update ot new_order in above loop
        if new_order_unit_count_updated != new_order.units:
            res = ddb_conn.put_item(
                TableName="LimitOrderBookTest",
                Item = new_order.to_ddb_item()
            )
            print("Partial filled: {}".format(new_order.to_ddb_item()))
            return {
                "statusCode": 200,
                "body":  "Order partially filled"
            }
    
    # if order fully filled no need to add to limitordertable
    return {
        "statusCode": 200,
        "body":  "Order fully filled"
    }

def get_best_matching_orders(ddb_conn, order: Order)->List[Order]:
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
        TableName="LimitOrderBookTest",
        KeyConditionExpression=f"Gsi1Pk = :stockNameAndType AND Sk {'>=' if order.order_type =='SELL' else '<='} :price")
    
    # if no matching orders found, add order to limit book
    if len(res["Items"]) == 0:
        ddb_conn.put_item(
            TableName="LimitOrderBookTest",
            Item=order.to_ddb_item()
        )
        return []
    
    best_matching_orders = map(lambda order: Order(order), res["Items"])
    # sort first by price reversed, then by timestamp. This nested sort in python will maintain
    # order of the prices first
    best_matching_orders = sorted(sorted(best_matching_orders, key=lambda x: x.order_time), key=lambda x: x.price, reverse=True if order.order_type =='SELL' else False)
    return best_matching_orders

def fill_order(best_matching: Order, order: Order)->Tuple[Order,Order]:
    filled_orders = order.units
    order.units = order.units - best_matching.units if order.units - best_matching.units >= 0 else 0 # might be neater with a math floor function
    best_matching.units = best_matching.units - filled_orders if best_matching.units - filled_orders >= 0 else 0

    return best_matching, order