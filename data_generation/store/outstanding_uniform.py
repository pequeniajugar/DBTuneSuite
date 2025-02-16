import random
import numpy as np
import pandas as pd  

random.seed(42)

# Generate Store Data in Batches
def generate_and_save_store(num_stores, batch_size, file_name):
    with open(file_name, "w") as f:
        f.write("storeid,name\n")
    
    for start in range(1, num_stores + 1, batch_size):
        end = min(start + batch_size, num_stores + 1)
        stores = [(i, f"Store{i}") for i in range(start, end)]
        df = pd.DataFrame(stores, columns=["storeid", "name"])
        df.to_csv(file_name, mode="a", index=False, header=False)
        print(f"Processed {end - 1}/{num_stores} stores...")

# Generate Item Data in Batches
def generate_and_save_item(num_items, batch_size, file_name):
    with open(file_name, "w") as f:
        f.write("itemnum,price\n")
    
    for start in range(1, num_items + 1, batch_size):
        end = min(start + batch_size, num_items + 1)
        items = [(i, round(random.uniform(1, 100), 2)) for i in range(start, end)]
        df = pd.DataFrame(items, columns=["itemnum", "price"])
        df.to_csv(file_name, mode="a", index=False, header=False)
        print(f"Processed {end - 1}/{num_items} items...")

# Generate Vendor Outstanding Data in Batches
def generate_and_save_vendorOutstanding(num_vendors, batch_size, file_name):
    with open(file_name, "w") as f:
        f.write("vendorid,amount\n")
    
    for start in range(1, num_vendors + 1, batch_size):
        end = min(start + batch_size, num_vendors + 1)
        vendor_data = [(i, round(random.uniform(99000, 100000), 2)) for i in range(start, end)]
        df = pd.DataFrame(vendor_data, columns=["vendorid", "amount"])
        df.to_csv(file_name, mode="a", index=False, header=False)
        print(f"Processed {end - 1}/{num_vendors} vendorOutstanding...")

# Generate Store Outstanding Data in Batches
def generate_and_save_storeOutstanding(store_df, batch_size, file_name):
    num_stores = len(store_df)
    selected_store_ids = random.sample(store_df["storeid"].tolist(), num_stores // 2)  

    with open(file_name, "w") as f:
        f.write("storeid,amount\n")
    
    for start in range(0, len(selected_store_ids), batch_size):
        end = min(start + batch_size, len(selected_store_ids))
        store_outstanding_data = [(store_id, round(random.uniform(9000, 10000), 2)) for store_id in selected_store_ids[start:end]]
        df = pd.DataFrame(store_outstanding_data, columns=["storeid", "amount"])
        df.to_csv(file_name, mode="a", index=False, header=False)
        print(f"Processed {end}/{len(selected_store_ids)} storeOutstanding...")

# Generate Uniformly Distributed Vendor IDs for Orders
def generate_uniform_vendors(num_orders, num_vendors):
    """Generate a uniformly distributed vendor ID list."""
    base_vendors = list(range(1, num_vendors + 1))
    vendor_ids = (base_vendors * (num_orders // len(base_vendors) + 1))[:num_orders]
    random.shuffle(vendor_ids)
    return vendor_ids  # Do NOT shuffle to maintain uniform distribution

# Generate Orders Data in Batches
def generate_and_save_orders(num_orders, num_items, num_stores, num_vendors, batch_size, file_name):
    """Generate `orders` table with uniformly distributed vendorid and save in batches."""
    
    vendor_ids = generate_uniform_vendors(num_orders, num_vendors)  # Keep vendor IDs uniformly distributed

    with open(file_name, "w") as f:
        f.write("ordernum,itemnum,quantity,storeid,vendorid\n")
    
    for start in range(0, num_orders, batch_size):
        end = min(start + batch_size, num_orders)
        batch_vendor_ids = vendor_ids[start:end]  # Keep vendor IDs in sequence

        batch_orders = [
            (i + 1, random.randint(1, num_items), random.randint(1, 10), random.randint(1, num_stores), batch_vendor_ids[i - start])
            for i in range(start, end)
        ]

        df = pd.DataFrame(batch_orders, columns=["ordernum", "itemnum", "quantity", "storeid", "vendorid"])
        df.to_csv(file_name, mode="a", index=False, header=False)
        print(f"Processed {end}/{num_orders} orders...")

# Get user input for number of orders
while True:
    try:
        num_orders = int(input("Enter the total number of orders to generate: "))
        if num_orders <= 0:
            print("Please enter a positive integer.")
        else:
            break
    except ValueError:
        print("Invalid input! Please enter an integer.")

# Compute related table sizes based on num_orders
num_stores = max(1, num_orders // 100)
num_items = max(1, int(num_orders / 2.5))
num_vendors = max(1, num_orders // 100)  # Ensure at least 1 vendor

# Set batch size
batch_size = 10000
exponent = int(np.log10(num_orders))

# Generate and Save Tables in Batches
generate_and_save_store(num_stores, batch_size, f"store_uniform_10_{exponent}.csv")
generate_and_save_item(num_items, batch_size, f"item_uniform_10_{exponent}.csv")
generate_and_save_vendorOutstanding(num_vendors, batch_size, f"vendorOutstanding_uniform_10_{exponent}.csv")

# Load store data for storeOutstanding generation
store_df = pd.read_csv(f"store_uniform_10_{exponent}.csv")
generate_and_save_storeOutstanding(store_df, batch_size, f"storeOutstanding_uniform_10_{exponent}.csv")

# Generate and Save Orders in Batches
generate_and_save_orders(num_orders, num_items, num_stores, num_vendors, batch_size, f"order_uniform_10_{exponent}.csv")

print("\nData successfully saved to CSV files!")
