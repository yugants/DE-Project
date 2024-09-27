import pandas as pd
from faker import Faker
from datetime import datetime
import random

# Initialize Faker with Indian locale
fake = Faker('en_IN')

# Define the number of rows you want to generate
num_rows = 25

# Initialize an empty list to store the rows
data = []

# Generate customer data
for _ in range(num_rows):
    customer_id = random.randint(1000, 9999)
    first_name = fake.first_name()
    last_name = fake.last_name()
    address = fake.address().replace('\n', ', ')  # Replace newlines in addresses
    pincode = fake.postcode()  # Generate a valid Indian postal code
    phone_number = fake.phone_number()  # Generate a random phone number
    customer_joining_date = fake.date_between_dates(date_start=datetime(2020, 1, 1), date_end=datetime(2023, 8, 20))

    # Append row data to the list
    data.append([customer_id, first_name, last_name, address, pincode, phone_number, customer_joining_date])

# Create a DataFrame from the generated data
df = pd.DataFrame(data, columns=['customer_id', 'first_name', 'last_name', 'address', 'pincode', 'phone_number', 'customer_joining_date'])

# Save the DataFrame to a CSV file
df.to_csv('./actual_data/dim_customer.csv', index=False)

print("CSV file 'customer.csv' created successfully.")
