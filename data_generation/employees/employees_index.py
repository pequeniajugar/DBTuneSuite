import random
import numpy as np
import pandas as pd
import math

random.seed(42)

def generate_employees(num_employees, batch_size=1_000_000, file_name="employeesindex.csv"):
    """
    Generates employee data in batches, ensuring 'ssnum' and 'hundreds1' are globally sorted.
    
    - Generates unique values for columns (`name`, `lat`, `long`, `hundreds1`, `hundreds2`).
    - 'ssnum' and 'hundreds1' are strictly sorted.
    - Other columns are shuffled within each batch.
    - Writes data in batches to reduce memory usage.
    """

    exponent = int(math.log10(num_employees))  # Extract log10 exponent for naming
    file_name = f"employeesindex_10_{exponent}.csv"

    unique_count = num_employees // 100  # Number of unique values

    # **Generate unique values**
    unique_names = [f"name{i}" for i in range(unique_count)]
    unique_lats = np.round(np.linspace(-90.0, 90.0, unique_count), 2)  # Evenly spaced latitudes
    unique_longs = np.round(np.linspace(-180.0, 180.0, unique_count), 2)  # Evenly spaced longitudes

    # **Generate globally sorted 'hundreds1'**
    global_hundreds1 = np.linspace(100, 999, num_employees, dtype=int)  # Fully sorted
    global_hundreds2 = global_hundreds1.copy()  # Keep identical to hundreds1

    # **Write header to file**
    with open(file_name, "w") as f:
        f.write("ssnum,name,lat,long,hundreds1,hundreds2\n")

    # **Process in batches**
    for batch_start in range(0, num_employees, batch_size):
        batch_end = min(batch_start + batch_size, num_employees)  # Define batch range
        batch_size_actual = batch_end - batch_start  # Actual batch size in this batch

        # **Generate sequential 'ssnum'**
        ssnums = np.arange(batch_start + 1, batch_end + 1)

        # **Extract corresponding sorted 'hundreds1'**
        sorted_hundreds1 = global_hundreds1[batch_start:batch_end]
        sorted_hundreds2 = global_hundreds2[batch_start:batch_end]

        # **Repeat unique values to fill batch**
        selected_names = np.tile(unique_names, (batch_size_actual // unique_count) + 1)[:batch_size_actual]
        selected_lats = np.tile(unique_lats, (batch_size_actual // unique_count) + 1)[:batch_size_actual]
        selected_longs = np.tile(unique_longs, (batch_size_actual // unique_count) + 1)[:batch_size_actual]

        # **Shuffle all columns except 'ssnum' and 'hundreds1'**
        shuffle_indices = np.arange(batch_size_actual)
        np.random.shuffle(shuffle_indices)

        selected_names = selected_names[shuffle_indices]
        selected_lats = selected_lats[shuffle_indices]
        selected_longs = selected_longs[shuffle_indices]
        selected_hundreds2 = sorted_hundreds2[shuffle_indices]  # Only shuffle hundreds2

        # **Create DataFrame**
        df = pd.DataFrame({
            "ssnum": ssnums,  # Already sorted
            "name": selected_names,
            "lat": selected_lats,
            "long": selected_longs,
            "hundreds1": sorted_hundreds1,  # Globally sorted
            "hundreds2": selected_hundreds2  # Shuffled
        })

        # **Append batch to CSV file**
        df.to_csv(file_name, mode="a", index=False, header=False)

        print(f"Batch {batch_start + 1} - {batch_end} written to file.")

    print(f"\nEmployee data successfully saved in {file_name}")

# **User input**
while True:
    try:
        num_employees = int(input("Enter the total number of employees to generate: "))
        if num_employees <= 0:
            print("Please enter a positive integer.")
        else:
            break
    except ValueError:
        print("Invalid input! Please enter an integer.")

# **Generate and store data**
generate_employees(num_employees)
