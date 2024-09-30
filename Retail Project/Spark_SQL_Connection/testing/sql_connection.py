import mysql.connector

# Establish the connection
conn = mysql.connector.connect(
    host="localhost",        # Replace with your host
    user="root",    # Replace with your MySQL username
    password="root", # Replace with your MySQL password
    database="test_db"  # Replace with the database you want to connect to
)

# Check if the connection was successful
if conn.is_connected():
    print("Connected to MySQL database")
else:
    print("Connection failed")

cursor = conn.cursor()


create_table_query = """
CREATE TABLE IF NOT EXISTS new_customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT,
    city VARCHAR(255)
);
"""

# Execute the SQL command
cursor.execute(create_table_query)

conn.commit()

# Execute SQL command
cursor.execute('SHOW TABLES')

# Print the results
for db in cursor:
    print(db)