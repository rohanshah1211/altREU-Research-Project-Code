import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import os
from datetime import timedelta


# Set Google Sheets API credentials and spreadsheet name
SERVICE_ACCOUNT_FILE = r"C:\Users\rohan\Downloads\dark-valor-426921-b2-03329f14e050.json"
SHEET_NAME = 'Stock Price Data - Utilities'

# Initialize VADER sentiment analyzer
vader_analyzer = SentimentIntensityAnalyzer()

# Function to scrape news articles from a website
def scrape_news(company_name, from_date, to_date):
    # Example: Scraping from a hypothetical website 'example.com'
    url = f'https://example.com/search?q={company_name}&from={from_date}&to={to_date}'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    articles = []

    # Modify this according to the website's structure
    for item in soup.find_all('div', class_='news-item'):
        title = item.find('h2').text
        description = item.find('p').text
        articles.append({'title': title, 'description': description})

    return articles

# Function to calculate sentiment using VADER
def calculate_sentiment(texts):
    scores = []
    for text in texts:
        sentiment_score = vader_analyzer.polarity_scores(text)['compound']
        scores.append(sentiment_score)
    return scores

# Function to update Google Sheets with sentiment scores
def update_google_sheet_with_sentiment(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(sheet_name)
    except gspread.SpreadsheetNotFound:
        print(f"Spreadsheet '{sheet_name}' not found. Please ensure the name is correct and the service account has access.")
        return

    for worksheet in sheet.worksheets():
        symbol = worksheet.title
        print(f"Processing {symbol}...")

        try:
            df_existing = pd.DataFrame(worksheet.get_all_records())
            df_existing['Date'] = pd.to_datetime(df_existing['Date'], utc=True)  # Ensure dates are parsed with UTC

            sentiment_scores = []

            for date in df_existing['Date']:
                from_date = date.strftime('%Y-%m-%d')
                to_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')
                articles = scrape_news(symbol, from_date, to_date)
                print(f"Fetched {len(articles)} articles for {symbol} from {from_date} to {to_date}")

                if articles:
                    texts = [article['title'] + ' ' + article['description'] for article in articles]
                    if texts:
                        scores = calculate_sentiment(texts)
                        avg_score = sum(scores) / len(scores)
                    else:
                        avg_score = 0
                else:
                    avg_score = 0

                sentiment_scores.append(avg_score)
                print(f"Processed date {date}: {avg_score}")

            df_existing['Sentiment Score'] = sentiment_scores

            # Convert all columns to string (if needed)
            df_existing = df_existing.astype(str)

            worksheet.clear()
            worksheet.update([df_existing.columns.values.tolist()] + df_existing.values.tolist())
            print(f"Updated worksheet '{symbol}' with sentiment scores.")
        except Exception as e:
            print(f"Error updating worksheet '{symbol}': {e}")

# Example usage
if __name__ == "__main__":
    update_google_sheet_with_sentiment(SHEET_NAME)
    print("Google Sheet updated successfully with sentiment scores.")
