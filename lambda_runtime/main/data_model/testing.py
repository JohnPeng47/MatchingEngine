template = {
    "Pk": {
        "S" : "1"
    },
    "Sk" : {
        "N" : "1"
    },
    "Gsi1Pk" : {
        "S" : "1"
    },
    "units" : {
        "N" : "1"
    },
    "order_time" : {
        "N" : "1"
    },
    "filled" : {
        "BOOL" : "1"
    },
    "user_id" : {
        "S" : "1"
    }
}

# generate 
for key, val in template.items():
    print(list(template[key].values())[0])