import pandas as pd
from faker import Faker
from datetime import datetime
import random

# Initialize Faker
fake = Faker()

# Define the number of rows you want to generate
num_rows = 100


customer_df = pd.read_csv('./actual_data/generated_csv/dim_customer.csv')
product_df = pd.read_csv('./actual_data/generated_csv/dim_product.csv')
sales_team_df = pd.read_csv('./actual_data/generated_csv/dim_sales_team.csv')
store_df = pd.read_csv('./actual_data/generated_csv/dim_store.csv')


customer_ids = customer_df['customer_id'].to_list()
store_ids = store_df['id'].to_list()
product_names = product_df['name'].to_list()
sales_person_ids = sales_team_df['id'].to_list()

# Initialize an empty list to store the rows
data = []

# Generate fact table data
for _ in range(num_rows):
    customer_id = random.choice(customer_ids)
    store_id = random.choice(store_ids)
    product_name = random.choice(product_names)
    sales_date = fake.date_between_dates(date_start=datetime(2020, 1, 1), date_end=datetime(2023, 8, 20))
    sales_person_id = random.choice(sales_person_ids)
    price = int(product_df[product_df['name'] == product_name]['current_price'].iloc[0])
    print(f'{product_name} = {price}')
    quantity = random.randint(1, 20)  # Random quantity between 1 and 20
    total_cost = round(price * quantity, 2)  # Total cost = price * quantity
    additional_column = fake.text(max_nb_chars=20)  # Additional information, up to 1000 chars

    # Append row data to the list
    data.append([customer_id, store_id, product_name, sales_date, sales_person_id, price, quantity, total_cost, additional_column])

# Create a DataFrame from the generated data
df = pd.DataFrame(data, columns=[
    'customer_id', 'store_id', 'product_name', 'sales_date', 
    'sales_person_id', 'price', 'quantity', 'total_cost', 'additional_column'
])

# Save the DataFrame to a CSV file
df.to_csv('./actual_data/generated_csv/fact_sales.csv', index=False)

print("CSV file 'fact_table.csv' created successfully.")
