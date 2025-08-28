# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import sqlite3
import pandas as pd
import numpy as np
import sys

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'Countries_by_GDP.csv'
log_file = '/home/project/etl_project_log.txt'


def log_progress(inp):
    """Log a timestamped message to the log file."""
    # Use valid directives: %b for month name (not %h)
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ' : ' + inp + '\n')


def extract(url, table_attribs):
    """
    Extract the required information from the website and return a dataframe.
    Cleans cell text and skips rows with non-numeric GDP values (e.g., dashes).
    """
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all('tbody')
    # This index is specific to the archived page layout used in the exercise
    rows = tables[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 and col[0].find('a') is not None:
            country = col[0].a.get_text(strip=True)
            gdp_raw = col[2].get_text(strip=True)  # e.g., "1,234" or "—" or "1,234[1]"
            # Only keep rows where the GDP cell has at least one digit
            if any(ch.isdigit() for ch in gdp_raw):
                df = pd.concat(
                    [df, pd.DataFrame({"Country": [country], "GDP_USD_millions": [gdp_raw]})],
                    ignore_index=True
                )
    return df


def transform(df):
    """
    Convert GDP from string with punctuation/footnotes to float (billions), rounded to 2 decimals.
    - Strips all non-numeric/non-dot characters.
    - Converts millions -> billions.
    - Drops rows that fail conversion.
    """
    cleaned = (
        df["GDP_USD_millions"]
        .astype(str)
        .str.replace(r'[^\d.]', '', regex=True)  # drop commas, spaces, footnote markers, dashes, etc.
    )

    gdp_millions = pd.to_numeric(cleaned, errors='coerce')
    df_out = df.copy()
    df_out["GDP_USD_billions"] = (gdp_millions / 1000).round(2)

    # Drop the original text column and any rows that couldn't be parsed
    df_out = df_out.drop(columns=["GDP_USD_millions"]).dropna(subset=["GDP_USD_billions"])
    # Optional: enforce types
    df_out["GDP_USD_billions"] = df_out["GDP_USD_billions"].astype(float)

    return df_out


def load_to_csv(df, csv_path):
    """Save the final dataframe as a CSV file."""
    df.to_csv(csv_path, index=False)


def load_to_db(df, sql_connection, table_name):
    """Save the final dataframe as a database table with the provided name."""
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    """Run the stated query on the database table and print the output."""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


# ----------------- Orchestration -----------------

try:
    log_progress('Preliminaries complete. Initiating ETL process.')

    df = extract(url, table_attribs)
    log_progress('Data extraction complete. Initiating transformation process.')

    df = transform(df)
    log_progress('Data transformation complete. Initiating loading process.')

    load_to_csv(df, csv_path)
    log_progress('Data saved to CSV file.')

    conn = sqlite3.connect(db_name)
    log_progress('SQL connection initiated.')

    load_to_db(df, conn, table_name)
    log_progress('Data loaded to database as table. Running the query.')

    query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"
    run_query(query_statement, conn)

    log_progress('Process complete.')
except Exception as e:
    log_progress(f'ERROR: {e}')
    print(f'ERROR: {e}', file=sys.stderr)
finally:
    try:
        conn.close()
    except Exception:
        pass
