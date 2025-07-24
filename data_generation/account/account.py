import random
import numpy as np
import pandas as pd
import math

random.seed(42)

# Generate and save account data in batches
def generate_and_save_accounts(num_accounts, batch_size):
    exponent = int(math.log10(num_accounts))  # Extract log10 exponent
    file_account = f"account_10_{exponent}.csv"
    file_account1 = f"account1_10_{exponent}.csv"
    file_account2 = f"account2_10_{exponent}.csv"
    chars = string.ascii_letters + string.digits

    ids = list(range(1, num_accounts + 1))

    # Write CSV headers
    with open(file_account, "w") as f:
        f.write("id,balance,homeaddress\n")
    with open(file_account1, "w") as f1:
        f1.write("id,balance\n")
    with open(file_account2, "w") as f2:
        f2.write("id,homeaddress\n")

    # Process data in batches
    for start in range(0, num_accounts, batch_size):
        end = min(start + batch_size, num_accounts)  # Compute batch end index
        batch_ids = ids[start:end]  # Get uniform IDs for this batch
        balances = np.round(np.random.uniform(5000, 10000, len(batch_ids)), 2)  # Generate balances in batch
        addresses = [str(''.join(random.choices(chars, k=2500))) for i in range(start, end)]    #Making a 2500 length address

        # Organize data
        batch_data = list(zip(batch_ids, balances, addresses))
        df = pd.DataFrame(batch_data, columns=["id", "balance", "homeaddress"])

        # Save full account data
        df.to_csv(file_account, mode="a", index=False, header=False)

        # Save account1 (id, balance)
        df1 = df[["id", "balance"]]
        df1.to_csv(file_account1, mode="a", index=False, header=False)

        # Save account2 (id, homeaddress)
        df2 = df[["id", "homeaddress"]]
        df2.to_csv(file_account2, mode="a", index=False, header=False)

        print(f"Written {end}/{num_accounts} records...")

    print(f"Data successfully saved in {file_account}, {file_account1}, and {file_account2}")

# Get user input for the number of accounts
while True:
    try:
        num_accounts = int(input("Enter the total number of accounts to generate: "))
        if num_accounts <= 0:
            print("Please enter a positive integer.")
        else:
            break
    except ValueError:
        print("Invalid input! Please enter an integer.")

batch_size = min(1000000, num_accounts)

generate_and_save_accounts(num_accounts, batch_size)
