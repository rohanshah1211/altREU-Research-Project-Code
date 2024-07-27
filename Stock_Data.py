import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Google Sheets API configuration
SERVICE_ACCOUNT_FILE = r"C:\Users\rohan\Downloads\dark-valor-426921-b2-03329f14e050.json"
SHEET_NAME = 'Stock Price Data - Technology'

# Function to fetch historical stock data for multiple symbols
def fetch_historical_data(symbols, start_date, end_date):
    data = {}
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        df = stock.history(start=start_date, end=end_date)
        df['Symbol'] = symbol  # Add symbol as a column for identification
        data[symbol] = df.copy()  # Ensure to make a copy of the DataFrame

    return data

# Function to calculate additional technical indicators
def calculate_technical_indicators(data):
    for symbol, df in data.items():
        # Simple Moving Average (SMA)
        df.loc[:, 'SMA_20'] = df['Close'].rolling(window=20).mean()

        # Exponential Moving Average (EMA)
        df.loc[:, 'EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()

        # Volatility (Standard Deviation)
        df.loc[:, 'Volatility_20'] = df['Close'].rolling(window=20).std()

        # Bollinger Bands
        df.loc[:, 'BB_upper'] = df['SMA_20'] + 2 * df['Volatility_20']
        df.loc[:, 'BB_lower'] = df['SMA_20'] - 2 * df['Volatility_20']

        # Average True Range (ATR)
        df.loc[:, 'High-Low'] = df['High'] - df['Low']
        df.loc[:, 'High-PrevClose'] = abs(df['High'] - df['Close'].shift(1))
        df.loc[:, 'Low-PrevClose'] = abs(df['Low'] - df['Close'].shift(1))
        df.loc[:, 'TR'] = df[['High-Low', 'High-PrevClose', 'Low-PrevClose']].max(axis=1)
        df.loc[:, 'ATR_14'] = df['TR'].rolling(window=14).mean()

        # Relative Strength Index (RSI)
        df.loc[:, 'Change'] = df['Close'].diff()
        df.loc[:, 'Gain'] = df['Change'].apply(lambda x: x if x > 0 else 0)
        df.loc[:, 'Loss'] = df['Change'].apply(lambda x: -x if x < 0 else 0)
        df.loc[:, 'Avg_Gain'] = df['Gain'].rolling(window=14).mean()
        df.loc[:, 'Avg_Loss'] = df['Loss'].rolling(window=14).mean()
        df.loc[:, 'RS'] = df['Avg_Gain'] / df['Avg_Loss']
        df.loc[:, 'RSI'] = 100 - (100 / (1 + df['RS']))

    return data

# Function to update Google Sheets with stock data including technical indicators
def update_google_sheet(data, sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(sheet_name)
    except gspread.SpreadsheetNotFound:
        print(f"Spreadsheet '{sheet_name}' not found. Please ensure the name is correct and the service account has access.")
        return

    for symbol, df in data.items():
        worksheet_name = f'{symbol}'
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            print(f"Worksheet '{worksheet_name}' not found, creating a new one.")
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")

        try:
            # Reverse the order of rows (most recent first)
            df = df.iloc[::-1].copy()  # Ensure a copy is made and reversed

            # Convert the index to a column and ensure all data is in a serializable format
            df.reset_index(inplace=True)
            df['Date'] = df['Date'].astype(str)  # Ensure the date is a string

            # Calculate additional technical indicators
            df = calculate_technical_indicators({symbol: df})[symbol]

            # Convert all Timestamp objects to string
            df = df.astype(str)

            worksheet.clear()
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())
            print(f"Updated worksheet '{worksheet_name}' with {len(df)} rows including technical indicators.")
        except Exception as e:
            print(f"Error updating worksheet '{worksheet_name}': {e}")

# Example usage
symbols = ['APPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'INTC', 'CSCO']  # List of symbols to fetch data for
start_date = '2014-01-01'
end_date = '2024-07-19'

data = fetch_historical_data(symbols, start_date, end_date)
update_google_sheet(data, SHEET_NAME)

print("Google Sheet updated successfully with additional technical indicators.")
