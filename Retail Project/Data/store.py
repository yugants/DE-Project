import pandas as pd
from faker import Faker
from datetime import datetime
import random

# Initialize Faker with Indian locale
fake = Faker('en_IN')

# Define the number of rows you want to generate
num_rows = 10

# Initialize an empty list to store the rows
data = []

# Generate store data
for _ in range(num_rows):
    store_id = random.randint(1, 100)  # Store IDs between 1 and 100
    address = fake.address().replace('\n', ', ')  # Replace newlines in address
    store_pincode = fake.postcode()  # Generate a valid Indian postal code
    store_manager_name = fake.name()  # Generate a random name for the store manager
    store_opening_date = fake.date_between_dates(date_start=datetime(2000, 1, 1), date_end=datetime(2023, 8, 20))
    reviews = fake.paragraph(nb_sentences=3)  # Generate a random review text with 3 sentences

    # Append row data to the list
    data.append([store_id, address, store_pincode, store_manager_name, store_opening_date, reviews])

# Create a DataFrame from the generated data
df = pd.DataFrame(data, columns=['id', 'address', 'store_pincode', 'store_manager_name', 'store_opening_date', 'reviews'])

# Save the DataFrame to a CSV file
df.to_csv('./actual_data/dim_store.csv', index=False)

print("CSV file 'store.csv' created successfully.")
