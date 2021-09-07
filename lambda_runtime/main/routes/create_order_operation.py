from lambda_runtime.main.data_model.order import Order

import json

def createOrderOperation(event):
    # returns best matching orders for the order type. Parameter is used to "paginate" the 
    order = Order(event["body"]["order"])
    ddb_conn = event["client"]
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
        return {
            "statusCode": 200,
            "body":  "Order successfully added to the table"
        }
    
    best_matching = map(lambda order: Order(order), res["Items"])
    # sort first by price reversed, then by timestamp. This nested sort in python will maintain
    # order of the prices first
    best_matching = sorted(sorted(best_matching, key=lambda x: x.order_time), key=lambda x: x.price, reverse=True if order.order_type =='SELL' else False)
    ret = list(map(lambda x: x.order_id, best_matching))
    
    return {
        "statusCode": 200,
        "body": ret
    }

# Design Question: break this function up into smaller pieces?
# Pros:
# -> easier to get test coverage
# -> can set a limit on get_best_matching_orders, optimization
# Cons:
# -> messy :(
    
# def get_best_matching_orders(ddb_client, order, i):
#     res = ddb_client.query(
#         Limit = 10 + i  * 10, # Optimization: limit the number of orders returned
#         ExpressionAttributeValues = {
#             ":price" : {
#                 "N" : str(order.price)
#             },
#             ":stockNameAndType" : {
#                 "S" : order.get_matching_pk()
#             }
#         },
#         IndexName="BestMatchingOffers",
#         TableName="LimitOrderBookTest",
#         KeyConditionExpression=f"Gsi1Pk = :stockNameAndType AND Sk {'>=' if order.order_type =='SELL' else '<='} :price")
    
#     print("ITEMS >> ", res["Items"])
#     for item in res["Items"]:
#         print("item", item)
    
#     # if no matching orders found, add full order to limit book
#     if len(res["Items"]) == 0:
#         ddb_client.put_item(
#             TableName="LimitOrderBookTest",
#             Item=order.to_ddb_item()
#         )
#         return {
#             "statusCode": 200,
#             "body":  "Order successfully added to the table"
#         }

    
#     for item in res["Items"]:
#         print(item)
    
#     best_matching = map(lambda order: Order(order), res["Items"])
#     # sort first by price reversed, then by timestamp. This nested sort in python will maintain
#     # order of the prices first
#     best_matching = sorted(sorted(best_matching, key=lambda x: x.order_time), key=lambda x: x.price, reverse=True if order.order_type =='SELL' else False)
#     for match in best_matching:
#         print(match.to_ddb_item())

#     # on consecutive calls of best_matching, just return the 20 latest 
#     return best_matching if i == 0 else best_matching[20:]



