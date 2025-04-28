import os
import pdfplumber
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import time
import warnings
import logging
logging.getLogger("pdfminer").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", message="CropBox missing from /Page, defaulting to MediaBox")

# PostgreSQL connection settings
DB_SETTINGS = {
    'dbname': 'postgres',
    'host': 'localhost',
    'port': 5432
}

# Folder path containing PDFs
PDF_FOLDER_PATH = '/Users/orgestbelba/Downloads/phone_numbers_telegram'
TABLE_NAME = 'phone_numbers_table'

# Connect to PostgreSQL
def connect_db():
    conn = psycopg2.connect(**DB_SETTINGS)
    print("Connected to PostgreSQL successfully.")
    return conn

# Create table if not exists
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                nid TEXT,
                name TEXT,
                surname TEXT,
                father_name TEXT,
                mother_name TEXT,
                dob TEXT,
                email TEXT,
                phone TEXT,
                data_source TEXT,
                insert_date TEXT
            );
        ''')
        conn.commit()

# Extract tables from PDF and return as list of rows
def extract_data_from_pdf(pdf_path):
    all_data = []
    headers = None  # Will store the header from the first page
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            for table in tables:
                if page_num == 0:
                    headers = table[0]  # First page: get headers
                    df = pd.DataFrame(table[1:], columns=headers)
                else:
                    if headers and len(table[0]) == len(headers):
                        df = pd.DataFrame(table, columns=headers)  # No header, use stored headers
                    else:
                        continue  # Skip malformed tables
                all_data.append(df)
                
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()

# Insert data into PostgreSQL
def insert_data(conn, df, batch_size=10000):
    if df.empty:
        return

    # Define the target schema
    target_columns = ['NID', 'NAME', 'SURNAME', 'FATHER_NAME', 'MOTHER_NAME', 'DOB', 'EMAIL', 'PHONE', 'DATA_SOURCE', 'INSERT_DATE']

    # Ensure DataFrame has all required columns, filling missing ones with None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    df = df[target_columns]  # Reorder columns

    with conn.cursor() as cur:
        for start in range(0, len(df), batch_size):
            end = start + batch_size
            batch = df.iloc[start:end]
            values = [tuple(x) for x in batch.to_numpy()]
            query = f'''
                INSERT INTO {TABLE_NAME} (nid, name, surname, father_name, mother_name, dob, email, phone, data_source, insert_date)
                VALUES %s
            '''
            execute_values(cur, query, values)
        conn.commit()

# Main function to process all PDFs in the folder
def process_pdfs(folder_path):
    conn = connect_db()
    create_table(conn)
    for filename in os.listdir(folder_path):
        if filename.endswith('.pdf'):
            file_path = os.path.join(folder_path, filename)
            print(f'Processing: {file_path}')
            start_time = time.time()
            df = extract_data_from_pdf(file_path)
            if not df.empty:
                insert_data(conn, df)
                elapsed_time = (time.time() - start_time) / 60
                print(f'Finished processing {file_path} in {elapsed_time:.2f} minutes. Inserted {len(df)} rows.')
            else:
                print(f'No data extracted from {file_path}.')
    conn.close()
    print("All PDFs processed and data inserted.")

if __name__ == '__main__':
    process_pdfs(PDF_FOLDER_PATH)