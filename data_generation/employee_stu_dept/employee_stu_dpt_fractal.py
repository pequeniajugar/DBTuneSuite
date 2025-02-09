import random
import numpy as np
import pandas as pd 
import math 

np.random.seed(42)

def calculate_num_dpts(num_employees):
    """Calculate the number of vendors based on num_orders."""
    if num_employees == 10**4:
        return 8002
    elif num_employees == 10**5:
        return 80001
    elif num_employees == 10**6:
        return 800001
    elif num_employees == 10**7:
        return 8000002
    elif num_employees == 10**8:
        return 80000003
    else:
        return max(1, num_employees // 10)  # Default case
    
# Generate a fixed list of tech departments with managers and locations.
def generate_and_save_techdept(num_employees, batch_size, file_name="techdept.csv"):
    num_dept = int(calculate_num_dpts(num_employees))  # Number of departments
    dept_names = [f"Dept{i}" for i in range(1, num_dept + 1)]  
    
    # Write CSV header
    with open(file_name, "w") as f:
        f.write("dept,manager,location\n")

    for start in range(1, num_dept + 1, batch_size):
        end = min(start + batch_size, num_dept + 1)
        batch_data = [(f"Dept{i}", f"Manager{i}", f"Location{i}") for i in range(start, end)]
        df = pd.DataFrame(batch_data, columns=["dept", "manager", "location"])
        df.to_csv(file_name, mode="a", index=False, header=False)
        print(f"Processed {end - 1}/{num_dept} tech departments...")

    print(f"\nTechDept data successfully saved in {file_name}")
    return dept_names  


# Generate fractally distributed IDs
def gen(frac, N):
    p = list(range(1, N + 1))
    random.shuffle(p)
    outvec = p[:]
    
    while len(p) > 1:
        p = p[:int(len(p) * frac)]
        outvec = p + outvec
    
    random.shuffle(outvec)
    return outvec

# Generate Employee Data in Batches
def generate_and_save_employees(num_employees, batch_size, file_name="employees.csv"):
    num_dpt = int(calculate_num_dpts(num_employees))
    depts = gen(0.2, num_dpt)  
    salaries = np.round(np.random.uniform(50000, 60000, num_employees), 2)  
    numfriends = np.random.randint(1, 50, num_employees)  
    
    # Write CSV header
    with open(file_name, "w") as f:
        f.write("ssnum,name,dept,salary,numfriends\n")
    
    for start in range(0, num_employees, batch_size):
        end = min(start + batch_size, num_employees)
        batch_data = [(i + 1, f"Name{i+1}", depts[i], salaries[i], numfriends[i]) for i in range(start, end)]
        df = pd.DataFrame(batch_data, columns=["ssnum", "name", "dept", "salary", "numfriends"])
        df.to_csv(file_name, mode="a", index=False, header=False)
        print(f"Processed {end}/{num_employees} employees...")

    print(f"\nEmployee data successfully saved in {file_name}")

# Generate Student Data in Batches
def generate_and_save_students(num_employees, batch_size, file_name="students.csv"):
    num_students = num_employees
    num_courses = max(1, num_employees // 100)
    courses = [f"course{i}" for i in range(1, num_courses + 1)]  
    grades = np.random.randint(0, 101, num_students)  # Generate all grades at once
    # get a random start point a
    a = np.random.randint(num_employees // 2, num_employees + 1)
    ssnums = np.arange(a, a + num_students)
  

    # Write CSV header
    with open(file_name, "w") as f:
        f.write("ssnum,name,course,grade\n")

    for start in range(0, num_students, batch_size):
        end = min(start + batch_size, num_students)
        batch_data = [(ssnums[i], f"Name{ssnums[i]}", random.choice(courses), grades[i]) for i in range(start, end)]
        df = pd.DataFrame(batch_data, columns=["ssnum", "name", "course", "grade"])
        df.to_csv(file_name, mode="a", index=False, header=False)
        print(f"Processed {end}/{num_students} students...")

    print(f"\nStudent data successfully saved in {file_name}")

# Get user input for the number of employees and students
while True:
    try:
        num_employees = int(input("Enter the total number of employees to generate: "))
        if num_employees <= 0:
            print("Please enter positive integers.")
        else:
            break
    except ValueError:
        print("Invalid input! Please enter integers.")

# Set batch size
batch_size = 1000000

# Generate and Save Tables in Batches
exponent = int(math.log10(num_employees))

dept_names = generate_and_save_techdept(num_employees, batch_size,f"techdept_fractal_10_{exponent}.csv")
generate_and_save_employees(num_employees, batch_size, f"employees_fractal_10_{exponent}.csv")
generate_and_save_students(num_employees, batch_size, f"students_fractal_10_{exponent}.csv")

print("\nAll data successfully saved!")
