import pandas as pd
import random
import numpy as np
from faker import Faker

fake = Faker('en_IN')

def make_data_dirty(df, missing_percentage=0.1, swap_percentage=0.1, corrupt_percentage=0.1):
    """
    This function introduces dirty data into a DataFrame by:
    1. Introducing missing values in random columns.
    2. Swapping values between random columns.
    3. Corrupting values in some columns (changing data types).
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        missing_percentage (float): Percentage of values to make missing.
        swap_percentage (float): Percentage of rows where column values will be swapped.
        corrupt_percentage (float): Percentage of rows where values will be corrupted.
    
    Returns:
        pd.DataFrame: The DataFrame with dirty data.
    """
    
    # Get the number of rows and columns
    n_rows, n_cols = df.shape

    # 1. Introduce missing values
    for col in df.columns:
        n_missing = int(missing_percentage * n_rows)
        missing_indices = random.sample(range(n_rows), n_missing)
        df.loc[missing_indices, col] = np.nan

    # 2. Swap values between random columns
    for _ in range(int(swap_percentage * n_rows)):
        row_idx = random.randint(0, n_rows - 1)
        col1, col2 = random.sample(df.columns.tolist(), 2)
        df.loc[row_idx, [col1, col2]] = df.loc[row_idx, [col2, col1]].values

    # 3. Corrupt values (change data types)
    for col in df.columns:
        if df[col].dtype in ['int64', 'float64']:  # If numeric column
            n_corrupt = int(corrupt_percentage * n_rows)
            corrupt_indices = random.sample(range(n_rows), n_corrupt)
            df.loc[corrupt_indices, col] = fake.word()  # Replace with random string
        elif df[col].dtype == 'object':  # If text column
            n_corrupt = int(corrupt_percentage * n_rows)
            corrupt_indices = random.sample(range(n_rows), n_corrupt)
            df.loc[corrupt_indices, col] = random.uniform(0, 1000)  # Replace with random float

    return df

# Load CSV files from the './actual_data/' location
customer_df = pd.read_csv('./actual_data/dim_customer.csv')
store_df = pd.read_csv('./actual_data/dim_store.csv')
product_df = pd.read_csv('./actual_data/dim_product.csv')
sales_team_df = pd.read_csv('./actual_data/dim_sales_team.csv')
fact_sales_df = pd.read_csv('./actual_data/fact_sales.csv')

# Make each table dirty
customer_dirty = make_data_dirty(customer_df)
store_dirty = make_data_dirty(store_df)
product_dirty = make_data_dirty(product_df)
sales_team_dirty = make_data_dirty(sales_team_df)
fact_sales_dirty = make_data_dirty(fact_sales_df)

# Save the dirty tables back to the './actual_data/' folder with new filenames
customer_dirty.to_csv('./actual_data/dim_customer_dirty.csv', index=False)
store_dirty.to_csv('./actual_data/dim_store_dirty.csv', index=False)
product_dirty.to_csv('./actual_data/dim_product_dirty.csv', index=False)
sales_team_dirty.to_csv('./actual_data/dim_sales_team_dirty.csv', index=False)
fact_sales_dirty.to_csv('./actual_data/fact_sales_dirty.csv', index=False)

print("Dirty CSV files created successfully in './actual_data/'.")
