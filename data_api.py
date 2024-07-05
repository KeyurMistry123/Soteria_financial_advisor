from sqlalchemy import create_engine, text, TimeSeries
import pandas as pd

from alpha_vantage.timeseries import TimeSeries


# Replace 'YOUR_API_KEY' with your actual Alpha Vantage API key
api_key = 'PKYBHFOZCI96OQBN'
ts = TimeSeries(key=api_key, output_format='pandas')

# Fetch real-time data for a specific stock (e.g., Apple Inc.)
data, meta_data = ts.get_quote_endpoint(symbol='AAPL')

# Create a Pandas DataFrame from the data
df = pd.DataFrame(data)

# Print the DataFrame with nice formatting
print(df.to_string(index=False))

# Database connection configuration
database_type = 'mysql+mysqlconnector'
user = 'root'
password = '1234'
host = 'localhost'
port = '3306'
database = 'market_data'

# Create an SQLAlchemy engine
engine = create_engine(f'{database_type}://{user}:{password}@{host}:{port}/{database}')

# Function to transform and ingest data into the database
def ingest_data(df):
    try:
        # Transform the DataFrame (transpose and reset index)
        df = df.T
        df.reset_index(inplace=True)
        df.columns = ['metric', 'value']

        # Create a dictionary from the DataFrame
        data_dict = df.set_index('metric').to_dict()['value']

        # Insert data into the database
        insert_query = text("""
        INSERT INTO stock_data (symbol, open, high, low, price, volume, latest_trading_day, previous_close, change_amount, change_percent)
        VALUES (:open, :high, :low, :price, :volume)
        """)

        values = {
            
            'open': float(data_dict['02. open']),
            'high': float(data_dict['03. high']),
            'low': float(data_dict['04. low']),
            'price': float(data_dict['05. price']),
            'volume': int(data_dict['06. volume']),
        }

        # Execute the insert query
        with engine.connect() as connection:
            connection.execute(insert_query, values)
            print("Data inserted successfully.")
            
    except Exception as e:
        print(f"Error during data insertion: {str(e)}")

# Example usage:
# Fetch data from Alpha Vantage and store it in 'df' variable
# df = fetch_data_from_alpha_vantage()

# Ingest the fetched data into the database
# ingest_data(df)
