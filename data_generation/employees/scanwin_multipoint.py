import numpy as np
import pandas as pd
import math
import os

def generate_scanwin_multipoint(num_employees, file_name = "employees_multipoint.csv"):
    """
    Generate a multipoint employee dataset with specific percentage-based value distributions.
    Column values are drawn from fixed ranges and repeated according to fixed percentages.
    """

    def generate_column(values, repeat):
        data = np.repeat(values, repeat)
        np.random.shuffle(data)
        return data

    # === Dynamically generate file name based on log10 scale ===
    exponent = int(math.log10(num_employees))
    file_name = f"scanwin_multipoint_10_{exponent}.csv"
    file_path = os.path.join(".", file_name)

    # === Generate data for each multipoint column ===
    onepercent_values = np.arange(1, 101)
    onepercent_repeat = int(0.01 * num_employees)
    onepercent1 = generate_column(onepercent_values, onepercent_repeat)
    onepercent2 = generate_column(onepercent_values, onepercent_repeat)

    fivepercent_values = np.arange(1, 21)
    fivepercent_repeat = int(0.05 * num_employees)
    fivepercent1 = generate_column(fivepercent_values, fivepercent_repeat)
    fivepercent2 = generate_column(fivepercent_values, fivepercent_repeat)

    tenpercent_values = np.arange(1, 11)
    tenpercent_repeat = int(0.1 * num_employees)
    tenpercent1 = generate_column(tenpercent_values, tenpercent_repeat)
    tenpercent2 = generate_column(tenpercent_values, tenpercent_repeat)

    twentypercent_values = np.arange(1, 6)
    twentypercent_repeat = int(0.2 * num_employees)
    twentypercent1 = generate_column(twentypercent_values, twentypercent_repeat)
    twentypercent2 = generate_column(twentypercent_values, twentypercent_repeat)

    # === Assemble into DataFrame and save to CSV ===
    df = pd.DataFrame({
        "onepercent1": onepercent1,
        "onepercent2": onepercent2,
        "fivepercent1": fivepercent1,
        "fivepercent2": fivepercent2,
        "tenpercent1": tenpercent1,
        "tenpercent2": tenpercent2,
        "twentypercent1": twentypercent1,
        "twentypercent2": twentypercent2,
    })

    df.to_csv(file_path, index=False)
    print(f"\n✅ Data successfully saved to: {file_path}")


# === User input ===
while True:
    try:
        num_employees = int(input("Enter the total number of employees to generate: "))
        if num_employees <= 0:
            print("❌ Please enter a positive integer.")
        elif num_employees % 100 != 0:
            print("❌ Please enter a value divisible by 100 (e.g. 10000, 20000).")
        else:
            break
    except ValueError:
        print("❌ Invalid input! Please enter an integer.")

# === Run generation ===
generate_scanwin_multipoint(num_employees)
