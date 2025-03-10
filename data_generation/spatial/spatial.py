import csv
import random

# **User input for the exact number of rows**
while True:
    try:
        NUM_ROWS = int(input("Enter the number of rows to generate: "))
        if NUM_ROWS <= 0:
            print("Please enter a positive integer.")
        else:
            break
    except ValueError:
        print("Invalid input! Please enter a valid integer.")

# Generate dynamic file names based on NUM_ROWS % 10
EX = NUM_ROWS % 10
FILE_WITH_GEOM = f"spatial_with10_{EX}.csv"
FILE_WITHOUT_GEOM = f"spatial_without10_{EX}.csv"

# Ensure that UNIQUE_VALUES follows the pattern NUM_ROWS / 100
UNIQUE_VALUES = NUM_ROWS // 100  

def generate_data(num_rows):
    """
    Generates a dataset where:
    - `a1` to `a10` have `NUM_ROWS / 100` unique values, each repeated 100 times.
    - `a1` maintains its order, while `a2` to `a10` are shuffled.
    - `geom_a3_a7` is created using `a3` and `a7` as a coordinate pair.
    """

    # Generate base values: Each unique value is repeated 100 times
    base_values = [i for i in range(1, UNIQUE_VALUES + 1) for _ in range(100)]

    # Generate `a1` (keeps original order)
    a1_values = base_values.copy()

    # Generate `a2` to `a10` and shuffle them
    a2_values = base_values.copy()
    a3_values = base_values.copy()
    a4_values = base_values.copy()
    a5_values = base_values.copy()
    a6_values = base_values.copy()
    a7_values = base_values.copy()
    a8_values = base_values.copy()
    a9_values = base_values.copy()
    a10_values = base_values.copy()

    random.shuffle(a2_values)
    random.shuffle(a3_values)
    random.shuffle(a4_values)
    random.shuffle(a5_values)
    random.shuffle(a6_values)
    random.shuffle(a7_values)
    random.shuffle(a8_values)
    random.shuffle(a9_values)
    random.shuffle(a10_values)

    # Write data to two separate CSV files
    with open(FILE_WITH_GEOM, mode="w", newline="") as file_geom, \
         open(FILE_WITHOUT_GEOM, mode="w", newline="") as file_no_geom:
        
        writer_geom = csv.writer(file_geom)
        writer_no_geom = csv.writer(file_no_geom)

        # Write column headers
        writer_geom.writerow(["a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "geom_a3_a7"])
        writer_no_geom.writerow(["a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10"])

        for i in range(num_rows):
            a1 = a1_values[i]
            a2 = a2_values[i]
            a3 = a3_values[i]
            a4 = a4_values[i]
            a5 = a5_values[i]
            a6 = a6_values[i]
            a7 = a7_values[i]
            a8 = a8_values[i]
            a9 = a9_values[i]
            a10 = a10_values[i]

            # Generate `geom_a3_a7` in "POINT(a3 a7)" format
            geom_a3_a7 = f"POINT({a3} {a7})"

            # Write to the file with `geom_a3_a7`
            writer_geom.writerow([a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, geom_a3_a7])

            # Write to the file without `geom_a3_a7`
            writer_no_geom.writerow([a1, a2, a3, a4, a5, a6, a7, a8, a9, a10])

    print(f"Data generation complete!")
    print(f"File with geometry saved as: {FILE_WITH_GEOM}")
    print(f"File without geometry saved as: {FILE_WITHOUT_GEOM}")

# Run data generation
generate_data(NUM_ROWS)
