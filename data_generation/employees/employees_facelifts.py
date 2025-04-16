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
    file_name = f"employees_facelifts_10_{exponent}.csv"

    unique_count = num_employees // 100  # Number of unique values

    # **Generate unique values**
    unique_names = [f"name{i}" for i in range(100_000_000,100_000_000+unique_count)]
    unique_hundreds1 = np.arange(100+100_000_000, 100_000_000+100 + unique_count, dtype=int)  # Unique sorted values
    unique_lats = np.copy(unique_hundreds1)  # Initially same as hundreds1
    unique_longs = np.copy(unique_hundreds1)  # Initially same as hundreds1

    # **Repeat unique values 100 times to get global sorted versions**
    global_hundreds1 = np.repeat(unique_hundreds1, 100)  # Sorted
    global_hundreds2 = global_hundreds1.copy()  # Will be shuffled
    global_lats = global_hundreds1.copy()  # Will be shuffled
    global_longs = global_hundreds1.copy()  # Will be shuffled

    # **Write header to file**
    with open(file_name, "w") as f:
        f.write("ssnum,name,lat,long,hundreds1,hundreds2\n")

    # **Process in batches**
    for batch_start in range(0, num_employees, batch_size):
        batch_end = min(batch_start + batch_size, num_employees)  # Define batch range
        batch_size_actual = batch_end - batch_start  # Actual batch size in this batch

        # **Generate sequential 'ssnum'**
        ssnums = np.arange(batch_start + 1 + 100_000_000,100_000_000+ batch_end + 1)

        # **Extract corresponding sorted 'hundreds1'**
        sorted_hundreds1 = global_hundreds1[batch_start:batch_end]
        sorted_hundreds2 = global_hundreds2[batch_start:batch_end]
        sorted_lats = global_lats[batch_start:batch_end]
        sorted_longs = global_longs[batch_start:batch_end]

        # **Repeat unique values to fill batch**
        selected_names = np.tile(unique_names, (batch_size_actual // unique_count) + 1)[:batch_size_actual]

        # **Shuffle all columns except 'ssnum' and 'hundreds1'**
        shuffle_indices_hundreds2 = np.arange(batch_size_actual)
        shuffle_indices_lats = np.arange(batch_size_actual)
        shuffle_indices_longs = np.arange(batch_size_actual)

        np.random.shuffle(shuffle_indices_hundreds2)
        np.random.shuffle(shuffle_indices_lats)
        np.random.shuffle(shuffle_indices_longs)

        selected_names = selected_names[shuffle_indices_hundreds2]  # Names shuffled like hundreds2
        selected_lats = sorted_lats[shuffle_indices_lats]  # Lat shuffled differently
        selected_longs = sorted_longs[shuffle_indices_longs]  # Long shuffled differently
        selected_hundreds2 = sorted_hundreds2[shuffle_indices_hundreds2]  # Shuffle hundreds2

        # **Create DataFrame**
        df = pd.DataFrame({
            "ssnum": ssnums,  # Already sorted
            "name": selected_names,
            "lat": selected_lats,  # Independent shuffle
            "long": selected_longs,  # Independent shuffle
            "hundreds1": sorted_hundreds1,  # Globally sorted
            "hundreds2": selected_hundreds2  # Shuffled independently
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
