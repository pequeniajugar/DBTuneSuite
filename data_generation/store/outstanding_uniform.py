import random
import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

def generate_datasets(n, batch_size, base_filename):
    k = n // 100  # Unique key count

    # === Generate store.csv ===
    store_ids = list(range(1, k + 1))
    store_data = [(sid, f"Store{sid}") for sid in store_ids]
    pd.DataFrame(store_data, columns=["storeid", "name"]).to_csv(f"store{base_filename}.csv", index=False)

    # === Generate item.csv ===
    item_nums = store_ids.copy()
    item_prices = (np.random.permutation(store_ids) % 100) + 1  # Shuffle and mod
    item_data = list(zip(item_nums, item_prices))
    pd.DataFrame(item_data, columns=["itemnum", "price"]).to_csv(f"item{base_filename}.csv", index=False)

    # === Generate vendorOutstanding.csv ===
    vendor_ids = store_ids.copy()
    vendor_amounts = np.random.permutation(store_ids)
    vendor_data = list(zip(vendor_ids, vendor_amounts))
    pd.DataFrame(vendor_data, columns=["vendorid", "amount"]).to_csv(f"vendorOutstanding{base_filename}.csv", index=False)

    # === Generate storeOutstanding.csv ===
    store_amounts = np.random.permutation(store_ids)
    store_out_data = list(zip(store_ids, store_amounts))
    pd.DataFrame(store_out_data, columns=["storeid", "amount"]).to_csv(f"storeOutstanding{base_filename}.csv", index=False)

    # === Generate orders.csv ===
    ordernum = list(range(1, n + 1))
    repeated_ids = store_ids * 100  # length = n
    assert len(repeated_ids) == n

    itemnums = repeated_ids.copy()
    storeids = repeated_ids.copy()
    vendorids = repeated_ids.copy()
    quantities = repeated_ids.copy()

    random.shuffle(itemnums)
    random.shuffle(storeids)
    random.shuffle(vendorids)
    random.shuffle(quantities)

    quantities = [(q % 100) + 1 for q in quantities]

    orders = list(zip(ordernum, itemnums, quantities, storeids, vendorids))
    pd.DataFrame(orders, columns=["ordernum", "itemnum", "quantity", "storeid", "vendorid"]).to_csv(f"orders{base_filename}.csv", index=False)

    return f"Generated datasets with {n} rows in orders and {k} unique keys."


if __name__ == "__main__":
    while True:
        try:
            n = int(input("Enter the total number of orders to generate (must be divisible by 100): "))
            if n > 0 and n % 100 == 0:
                break
            else:
                print("Please enter a positive integer that is divisible by 100.")
        except ValueError:
            print("Invalid input! Please enter an integer.")

    batch_size = 10000
    base_filename = f"_10_{int(np.log10(n))}"
    message = generate_datasets(n=n, batch_size=batch_size, base_filename=base_filename)
    print(message)
