import random
import numpy as np
import pandas as pd
import math

random.seed(42)

def generate_employees(num_employees, batch_size=1_000_000, file_name="employeesindex.csv"):
    """
    Generates employee data in batches, ensuring 'ssnum' and 'hundreds1' are globally sorted.

    - 'ssnum' and 'hundreds1' are sorted.
    - Adds two shuffled columns: 'ssnumpermuted1', 'ssnumpermuted2'.
    - Other columns (name, lat, long, hundreds2) are shuffled within each batch.
    - Data is written in batches to reduce memory usage.
    """

    exponent = int(math.log10(num_employees))
    file_name = f"employeesindex_10_{exponent}.csv"

    unique_count = num_employees // 100  # number of unique values

    # Generate unique values
    unique_names = [f"name{i}" for i in range(unique_count)]
    unique_hundreds1 = np.arange(100, 100 + unique_count, dtype=int)
    unique_lats = np.copy(unique_hundreds1)
    unique_longs = np.copy(unique_hundreds1)

    global_hundreds1 = np.repeat(unique_hundreds1, 100)
    global_hundreds2 = global_hundreds1.copy()
    global_lats = global_hundreds1.copy()
    global_longs = global_hundreds1.copy()

    # Write CSV header
    with open(file_name, "w") as f:
        f.write("ssnum,ssnumpermuted1,ssnumpermuted2,name,lat,long,hundreds1,hundreds2\n")

    # Process in batches
    for batch_start in range(0, num_employees, batch_size):
        batch_end = min(batch_start + batch_size, num_employees)
        batch_size_actual = batch_end - batch_start

        ssnums = np.arange(batch_start + 1, batch_end + 1)
        sorted_hundreds1 = global_hundreds1[batch_start:batch_end]
        sorted_hundreds2 = global_hundreds2[batch_start:batch_end]
        sorted_lats = global_lats[batch_start:batch_end]
        sorted_longs = global_longs[batch_start:batch_end]

        selected_names = np.tile(unique_names, (batch_size_actual // unique_count) + 1)[:batch_size_actual]

        # Shuffle individual columns
        shuffle_indices_hundreds2 = np.arange(batch_size_actual)
        shuffle_indices_lats = np.arange(batch_size_actual)
        shuffle_indices_longs = np.arange(batch_size_actual)

        np.random.shuffle(shuffle_indices_hundreds2)
        np.random.shuffle(shuffle_indices_lats)
        np.random.shuffle(shuffle_indices_longs)

        selected_names = selected_names[shuffle_indices_hundreds2]
        selected_hundreds2 = sorted_hundreds2[shuffle_indices_hundreds2]
        selected_lats = sorted_lats[shuffle_indices_lats]
        selected_longs = sorted_longs[shuffle_indices_longs]

        # Generate two independently permuted versions of ssnum
        ssnumpermuted1 = np.random.permutation(ssnums)
        ssnumpermuted2 = np.random.permutation(ssnums)

        df = pd.DataFrame({
            "ssnum": ssnums,
            "ssnumpermuted1": ssnumpermuted1,
            "ssnumpermuted2": ssnumpermuted2,
            "name": selected_names,
            "lat": selected_lats,
            "long": selected_longs,
            "hundreds1": sorted_hundreds1,
            "hundreds2": selected_hundreds2
        })

        df.to_csv(file_name, mode="a", index=False, header=False)
        print(f"Batch {batch_start + 1} - {batch_end} written to file.")

    print(f"\nEmployee data successfully saved to {file_name}")

# Prompt user input
while True:
    try:
        num_employees = int(input("Enter the total number of employees to generate: "))
        if num_employees <= 0:
            print("Please enter a positive integer.")
        else:
            break
    except ValueError:
        print("Invalid input! Please enter an integer.")

# Generate data
generate_employees(num_employees)
