import random
import numpy as np
import pandas as pd  
import math

random.seed(42)

# Generate employee records in batches and save to CSV
def generate_employees(num_employees, batch_size=10000, file_name="employees.csv"):
    exponent = int(math.log10(num_employees))  # Extract log10 exponent
    file_name = f"employees_10_{exponent}.csv"

    # Open the file and write headers
    with open(file_name, "w") as f:
        f.write("ssnum,name,lat,long,hundreds1,hundreds2\n")  # Write header once

    # Process in batches
    for start in range(1, num_employees + 1, batch_size):
        end = min(start + batch_size - 1, num_employees)  # Calculate batch range
        
        # Generate unique employee IDs for the batch
        ssnums = list(range(start, end + 1))

        # Generate employee names
        names = [f"Employee{i}" for i in ssnums]

        # Generate random latitude and longitude values (global range)
        lats = np.round(np.random.uniform(-90.0, 90.0, len(ssnums)), 2)  
        longs = np.round(np.random.uniform(-180.0, 180.0, len(ssnums)), 2)  

        # Generate random values in hundreds
        hundreds1 = np.random.randint(100, 1000, len(ssnums))  
        hundreds2 = np.random.randint(100, 1000, len(ssnums))  

        # Create list of tuples for employees
        batch_data = list(zip(ssnums, names, lats, longs, hundreds1, hundreds2))

        # Convert to DataFrame
        df = pd.DataFrame(batch_data, columns=["ssnum", "name", "lat", "long", "hundreds1", "hundreds2"])

        # Append to CSV (without writing the header again)
        df.to_csv(file_name, mode="a", index=False, header=False)

    print(f"\nEmployee data successfully saved in {file_name}")

# Get user input for the number of employees
while True:
    try:
        num_employees = int(input("Enter the total number of employees to generate: "))
        if num_employees <= 0:
            print("Please enter a positive integer.")
        else:
            break
    except ValueError:
        print("Invalid input! Please enter an integer.")

# Generate and save employees in batches
generate_employees(num_employees)
