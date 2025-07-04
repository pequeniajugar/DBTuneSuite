import pandas as pd
import numpy as np
import random

random.seed(42)
np.random.seed(42)

shuffled_itemnums = np.random.permutation(range(1, 1001))
shuffled_storeids = np.random.permutation(range(1, 1001))
shuffled_vendorids = np.random.permutation(range(1, 1001))

data = []
for i in range(1000):
    order_id = 100000000 + i
    itemnum = shuffled_itemnums[i]
    quantity = 500 + (i % 100)
    storeid = shuffled_storeids[i]
    vendorid = shuffled_vendorids[i]
    data.append([order_id, itemnum, quantity, storeid, vendorid])

df = pd.DataFrame(data, columns=["order_id", "itemnum", "quantity", "storeid", "vendorid"])
df.to_csv("triggers_input.csv", index=False)

print("CSV file 'triggers_input.csv' has been created.")
