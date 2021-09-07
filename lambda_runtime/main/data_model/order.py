from enum import Enum
from datetime import datetime
import boto3
import uuid

class Order:
    def __init__(self, order, mode="create", original=None):
        # order from dynamodb Item
        if "Pk" in order.keys():
            mode = "Dynamo"
            self.order_id: str = order["Pk"]["S"]
            self.user_id: str = order["user_id"]["S"]
            stock_symbol, order_type = self.get_stock_symbol_from_ddbitem(order)
            self.stock_symbol: str = stock_symbol
            self.order_type: str = order_type
            self.units: int = int(order["units"]["N"])
            self.price: float = float(order["Sk"]["N"])
            self.order_time: float = float(order["order_time"]["N"])
            self.filled: bool = order["filled"]["BOOL"]
            
        # create new request
        elif mode == "create":
            # renamed id to order_id from specs PDF
            self.order_id: str = str(uuid.uuid4())
            self.user_id: str = order["user_id"]
            self.stock_symbol: str = order["stock_symbol"]
            self.order_type: str = order["order_type"]
            self.units: int = order["units"]
            self.price: float = order["price"]
            self.order_time: float = datetime.utcnow().timestamp()
            self.filled: bool = False

        # update
        elif mode == "update" and original:
            # we don't let them update order_id, user_id or stock_symbol
            self.order_id: str = original["Pk"]["S"]
            self.user_id: str = original["user_id"]["S"]
            stock_symbol, order_type = self.get_stock_symbol_from_ddbitem(original)
            self.stock_symbol: str = stock_symbol
            self.order_type: str = order_type
            self.units: int = int(original["units"]["N"]) if not order.get("units", None) else order.get("units")
            self.price: float = float(original["Sk"]["N"]) if not order.get("price", None) else order.get("price")
            self.order_time: float = datetime.utcnow().timestamp()
            self.filled: bool = True

    def get_stock_symbol_from_ddbitem(self, order):
        return order["Gsi1Pk"]["S"].split("_")[0], order["Gsi1Pk"]["S"].split("_")[1]
            
    def get_matching_pk(self):
        t = "SELL" if self.order_type == "BUY" else "BUY"
        return self.stock_symbol + "_" + t

    def to_ddb_item(self):
        ddb_item = {
            "Pk": {
                "S" : self.order_id
            },
            "Sk" : {
                "N" : str(self.price)
            },
            "Gsi1Pk" : {
                "S" : self.stock_symbol + "_" + self.order_type
            },
            "units" : {
                "N" : str(self.units)
            },
            "order_time" : {
                "N" : str(self.order_time)
            },
            "filled" : {
                "BOOL" : self.filled
            },
            "user_id" : {
                "S" : self.user_id
            }
        }
        return ddb_item

        # update_item = {} 
        # # for put_item calls, which only want to update certain fields
        # for key, val in template.items():
        #     if list(template[key].values())[0]:
        #         update_item[key] = val
        
        # return update_item