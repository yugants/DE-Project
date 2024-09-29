import pandas as pd
from faker import Faker
from datetime import datetime
import random

# Initialize Faker with Indian locale
fake = Faker('en_IN')

# Define the number of rows you want to generate
num_rows = 20

# Initialize an empty list to store the rows
data = []

# Generate sales team data
for i in range(1, num_rows + 1):
    first_name = fake.first_name()
    last_name = fake.last_name()
    
    # Randomly assign managers; about 10% will be managers
    is_manager = 'Y' if random.random() < 0.1 else 'N'
    
    # Assign manager_id only if not a manager (otherwise set to None)
    manager_id = random.randint(1, num_rows) if is_manager == 'N' else None
    
    address = fake.address().replace('\n', ', ')
    pincode = fake.postcode()
    joining_date = fake.date_between_dates(date_start=datetime(2015, 1, 1), date_end=datetime(2023, 8, 20))
    
    # Append row data to the list
    data.append([i, first_name, last_name, manager_id, is_manager, address, pincode, joining_date])

# Create a DataFrame from the generated data
df = pd.DataFrame(data, columns=['id', 'first_name', 'last_name', 'manager_id', 'is_manager', 'address', 'pincode', 'joining_date'])

# Save the DataFrame to a CSV file
df.to_csv('./actual_data/generated_csv/dim_sales_team.csv', index=False)

print("CSV file 'sales_team.csv' created successfully.")
