import pandas as pd

# Generate 1000 rows of data
data = []
for i in range(1000):
    order_id = 100000000 + i
    itemnum = 7000 + (i % 100)       # Cycle through 100 different itemnums
    quantity = 500 + (i % 100)       # Vary quantities
    storeid = f"xxxxxx{i:04d}"       # Padded storeid
    vendorid = f"vendor{i % 10}"     # Cycle through 10 vendors
    data.append([order_id, itemnum, quantity, storeid, vendorid])

df = pd.DataFrame(data, columns=["order_id", "itemnum", "quantity", "storeid", "vendorid"])
df.to_csv("triggers_input.csv", index=False)

print("CSV file 'triggers_input.csv' has been created.")
