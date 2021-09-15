import uuid
# define decorator for logging transactions we care about
def log_transaction(ddb_client, stock_symbol, buy_order_id, sell_order_id, units, price):
    transaction_id = uuid.uuid4()
    transaction = {
        "Pk": {
            "S": transaction_id
        },
        "stock_symbol": {
        "S": stock_symbol
        },
        "buy_order_id" : {
        "S": buy_order_id},
        "sell_order_id" : {
        "S": sell_order_id},
        "unit":{
        "N": units},
        "price": {
            "N": price
        }
    }
    ddb_client.put_item(
        TableName="TransactionTable",
        Item=transaction
    )