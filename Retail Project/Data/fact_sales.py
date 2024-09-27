import pandas as pd
from faker import Faker
from datetime import datetime
import random

# Initialize Faker
fake = Faker()

# Define the number of rows you want to generate
num_rows = 50

# Define sample data based on the dimension tables we've created
# Assuming we already have 20 customers, 10 stores, 20 products, and 20 salespersons
customer_ids = list(range(1, 21))  # Customer IDs from 1 to 20
store_ids = list(range(1, 11))  # Store IDs from 1 to 10
product_names = [
    "Rice", "Wheat Flour", "Pulses", "Sugar", "Milk", 
    "Butter", "Bread", "Eggs", "Vegetable Oil", 
    "Salt", "Toothpaste", "Shampoo", "Soap", "Detergent",
    "Biscuits", "Soft Drinks", "Juice", "Frozen Vegetables", 
    "Tea", "Coffee"
]
sales_person_ids = list(range(1, 21))  # Salesperson IDs from 1 to 20

# Initialize an empty list to store the rows
data = []

# Generate fact table data
for _ in range(num_rows):
    customer_id = random.choice(customer_ids)
    store_id = random.choice(store_ids)
    product_name = random.choice(product_names)
    sales_date = fake.date_between_dates(date_start=datetime(2020, 1, 1), date_end=datetime(2023, 8, 20))
    sales_person_id = random.choice(sales_person_ids)
    price = round(random.uniform(10, 500), 2)  # Random price between 10 and 500
    quantity = random.randint(1, 20)  # Random quantity between 1 and 20
    total_cost = round(price * quantity, 2)  # Total cost = price * quantity
    additional_column = fake.text(max_nb_chars=200)  # Additional information, up to 1000 chars

    # Append row data to the list
    data.append([customer_id, store_id, product_name, sales_date, sales_person_id, price, quantity, total_cost, additional_column])

# Create a DataFrame from the generated data
df = pd.DataFrame(data, columns=[
    'customer_id', 'store_id', 'product_name', 'sales_date', 
    'sales_person_id', 'price', 'quantity', 'total_cost', 'additional_column'
])

# Save the DataFrame to a CSV file
df.to_csv('./actual_data/fact_sales.csv', index=False)

print("CSV file 'fact_table.csv' created successfully.")
