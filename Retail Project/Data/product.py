import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
import random

# Initialize Faker
fake = Faker()

# Define a list of common products sold in stores like Reliance Fresh
product_names = [
    "Rice", "Wheat Flour", "Pulses", "Sugar", "Milk", 
    "Butter", "Bread", "Eggs", "Vegetable Oil", 
    "Salt", "Toothpaste", "Shampoo", "Soap", "Detergent",
    "Biscuits", "Soft Drinks", "Juice", "Frozen Vegetables", 
    "Tea", "Coffee"
]

# Define the number of rows you want to generate
num_rows = 20

# Initialize an empty list to store the rows
data = []

# Generate product data
for _ in range(num_rows):
    product_id = random.randint(1, 1000)  # Product ID between 1 and 1000
    product_name = random.choice(product_names)  # Random product name from the list
    current_price = round(random.uniform(10, 500), 2)  # Random current price between 10 and 500
    old_price = round(current_price + random.uniform(5, 100), 2)  # Random old price slightly higher than current
    created_date = fake.date_time_between(start_date='-3y', end_date='now')  # Product created in the last 3 years
    updated_date = fake.date_time_between(start_date=created_date, end_date='now')  # Updated after creation date
    expiry_date = fake.date_between_dates(date_start=datetime.now(), date_end=datetime.now() + timedelta(days=365))  # Expiry within next year

    # Append row data to the list
    data.append([product_id, product_name, current_price, old_price, created_date, updated_date, expiry_date])

# Create a DataFrame from the generated data
df = pd.DataFrame(data, columns=['id', 'name', 'current_price', 'old_price', 'created_date', 'updated_date', 'expiry_date'])

# Save the DataFrame to a CSV file
df.to_csv('./actual_data/dim_product.csv', index=False)

print("CSV file 'product.csv' created successfully.")
