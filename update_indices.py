import yfinance as yf
from yfinance import Ticker
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os

# List of indices to fetch
INDICES = {
    "ES=F": "E_Mini_SP_500"
}


def initialize_table(symbol):
    """
    Ensure the table for a specific index exists in the database.
    """
    table_name = INDICES[symbol]#.replace('^', '').replace('-', '_')
    try:
        connection = pymysql.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            database=os.environ['DB_NAME'],
            cursorclass=pymysql.cursors.DictCursor, 
            connect_timeout=5
        )
        with connection.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    Date DATE PRIMARY KEY,
                    Open FLOAT,
                    High FLOAT,
                    Low FLOAT,
                    Close FLOAT,
                    Adj_Close FLOAT,
                    Volume INT
                )
            """)
        connection.commit()
        print(f"Table `{table_name}` initialized successfully.")
    except pymysql.MySQLError as e:
        print(f"Error initializing table `{table_name}`: {e}")
    finally:
        connection.close()


def table_has_data(symbol):
    """
    Check if data exists for a specific index table.
    """
    table_name = INDICES[symbol]#.replace('^', '').replace('-', '_')
    try:
        connection = pymysql.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            database=os.environ['DB_NAME'],
            cursorclass=pymysql.cursors.DictCursor, 
            connect_timeout=5
        )
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*) AS count FROM `{table_name}`
            """)
            result = cursor.fetchone()
            return result['count'] > 0
    except pymysql.MySQLError as e:
        print(f"Error checking data for table `{table_name}`: {e}")
        return False
    finally:
        connection.close()


def fetch_and_insert_data(symbol, period):
    """
    Fetch and insert data into a specific table for the given index.
    """
    table_name = INDICES[symbol]#.replace('^', '').replace('-', '_')
    try:
        engine = create_engine(f"mysql+pymysql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}")
        conn = engine.connect()
        
        print(f"Fetching data for {table_name} into table `{table_name}`...")
        ticker = Ticker(str(symbol))  # Ensure symbol is string
        if period == 'max':
            start_date = '1998-02-26'
            df = ticker.history(start=start_date, auto_adjust=False)
            print(df.head())
        else:
            df = ticker.history(period='1d', auto_adjust=False)

        if df.empty:
            print(f"No data found for {table_name}. Skipping...")
            return

        df.reset_index(inplace=True)
        print(df.head())
        
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        df.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
        
        # Insert into specific table
        df.to_sql(table_name, con=conn, if_exists='append', index=False)
        print(f"Data for `{table_name}` inserted successfully into `{table_name}`.")
    
    except Exception as e:
        print(f"Error fetching or inserting data for `{table_name}`: {e}")
    finally:
        conn.close()
        print(f"Connection closed for `{table_name}`.")


def get_all_dates():
    """
    Identify all dates across all index tables and find the maximum range.
    """
    try:
        connection = pymysql.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            database=os.environ['DB_NAME'],
            cursorclass=pymysql.cursors.DictCursor, 
            connect_timeout=5
        )
        all_dates = set()
        with connection.cursor() as cursor:
            for symbol in INDICES.keys():
                table_name = INDICES[symbol]#.replace('^', '').replace('-', '_')
                cursor.execute(f"SELECT Date FROM `{table_name}`")
                dates = {row['Date'] for row in cursor.fetchall()}
                all_dates.update(dates)
        return sorted(all_dates)
    except pymysql.MySQLError as e:
        print(f"Error retrieving dates: {e}")
        return []
    finally:
        connection.close()


def fill_missing_dates():
    """
    Fill missing dates in each table with the last available price.
    """
    all_dates = get_all_dates()
    if not all_dates:
        print("No dates found across tables.")
        return
    
    all_dates = sorted(set(all_dates))
    
    engine = create_engine(f"mysql+pymysql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}")
    
    for symbol in INDICES.keys():
        table_name = INDICES[symbol]#.replace('^', '').replace('-', '_')
        print(f"Filling missing dates for `{table_name}`...")
        df = pd.read_sql(f"SELECT * FROM `{table_name}`", con=engine)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.drop_duplicates(subset=['Date'])
        df.set_index('Date', inplace=True)
        
        # Reindex with the full date range
        df = df.reindex(pd.to_datetime(all_dates).date, method='ffill')
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'Date'}, inplace=True)
        print(df.head())
        print(len(df.index))
        # Update table
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Table `{table_name}` updated with filled dates.")


def main():
    """
    Main workflow:
    1. Initialize a table for each index.
    2. Fetch historical data if the table is empty.
    3. Fetch daily data if historical data already exists.
    4. Fill missing dates across all tables.
    """
    for symbol in INDICES.keys():
        print(f"Initializing table for {INDICES[symbol]}...")
        initialize_table(symbol)
        
        if not table_has_data(symbol):
            print(f"No data found for {INDICES[symbol]}. Fetching historical data...")
            fetch_and_insert_data(symbol, 'max')
        else:
            print(f"Data already exists for {INDICES[symbol]}. Fetching today's data...")
            fetch_and_insert_data(symbol, '1d')
    
    # Fill missing dates
    #fill_missing_dates()


if __name__ == '__main__':
    main()
