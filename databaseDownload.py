import pymssql
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("server")
USER = os.getenv("user")
PASSWORD = os.getenv("password")
DATABASE = os.getenv("database")

DB_CONFIG = {
    'server': SERVER,
    'user': USER,
    'password': PASSWORD,
    'database': DATABASE,
}


def export_all_tables_to_csv():

    conn = pymssql.connect(**DB_CONFIG)

    table_query = "SELECT name FROM sys.tables"
    tables_df = pd.read_sql(table_query, conn)
    output_dir = "exported_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for table_name in tables_df['name']:
        query = f"SELECT * FROM [dbo].[{table_name}]"
        df = pd.read_sql(query, conn)
        file_path = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(file_path, index=False)

    conn.close()


if __name__ == "__main__":
    export_all_tables_to_csv()
