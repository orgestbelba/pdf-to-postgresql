# PDF to PostgreSQL

This project extracts tabular data from PDF files and inserts it into a PostgreSQL database.

## Features
- Extracts tables from PDF files using `pdfplumber`.
- Processes multiple PDF files from a specified folder.
- Inserts extracted data into a PostgreSQL table.
- Handles batch inserts for efficiency.
- Automatically creates the target table if it doesn't exist.

## Requirements
- Python 3.x
- PostgreSQL
- Required Python packages:
  - pdfplumber
  - psycopg2
  - pandas

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-username/pdf-to-postgresql.git
   cd pdf-to-postgresql
   ```

2. Install dependencies using uv:
   ```
   uv sync
   ```

3. Set up your PostgreSQL database and ensure it is running.

## Configuration

Edit the `DB_SETTINGS` dictionary in `main.py` to match your PostgreSQL setup:
```python
DB_SETTINGS = {
    'dbname': 'postgres',
    'host': 'localhost',
    'port': 5432
}
```

Set the `PDF_FOLDER_PATH` variable to the folder containing your PDF files:
```python
PDF_FOLDER_PATH = '/path/to/your/pdf/folder'
```

## Usage

Run the script:
```
python main.py
```

The script will process all PDF files in the specified folder, extract table data, and insert it into the PostgreSQL database.

## License
This project is licensed under the MIT License.