
import psycopg2
import pandas

try:
    connection = psycopg2.connect(
        dbname="test",
        user="shubham",
        password="shubham",
        host="127.0.0.1",
        port="5432"
    )

    cursor = connection.cursor()

    df = pandas.read_csv('/home/shubham/Downloads/MOCK_DATA 1.csv')

    query = "INSERT INTO nmock (id, first_name, last_name, email, gender) VALUES (%s, %s, %s, %s, %s);"

    batch_size = 100

    for i in range(0, len(df), batch_size):
        batch = df[i:i+batch_size]
        records = [tuple(record) for record in batch.values]
        cursor.executemany(query, records)
        connection.commit()

    print("Data inserted in batches successfully.")

    connection.close()
except Exception as e:
    print(e)