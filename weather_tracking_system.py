import requests
import json
import sqlite3
import pandas as pd
from datetime import datetime
import os
from pathlib import Path
import time

class WeatherAPI:
    """Handles all weather API interactions"""

    def __init__(self, api_key):
        """Initialize with OpenWeatherMap API key"""
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"

    def fetch_weather(self, pincode):
        """
        Fetch weather data for given pincode
        Returns: Weather data dictionary or None if request fails
        """
        params = {
            "zip": f"{pincode},IN",
            "appid": self.api_key,
            "units": "metric"  # Use Celsius
        }

        try:
            response = requests.get(self.base_url, params=params, timeout=10)

            if response.status_code == 200:
                print("Successfully fetched weather data!")
                return response.json()
            else:
                print(f"Error: API request failed with status code {response.status_code}")
                return None

        except requests.RequestException as e:
            print(f"Error: Failed to fetch weather data - {str(e)}")
            return None

class WeatherDatabase:
    """Handles database operations"""

    def __init__(self, db_name="weather_data.db"):
        """Initialize database connection"""
        self.db_name = db_name
        self._create_tables()

    def _create_tables(self):
        """Create necessary database tables"""
        with sqlite3.connect(self.db_name) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS weather_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pincode TEXT NOT NULL,
                    location TEXT NOT NULL,
                    temperature REAL,
                    humidity INTEGER,
                    description TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')

    def save_weather_data(self, pincode, data):
        """Save weather data to database"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                conn.execute('''
                    INSERT INTO weather_records
                    (pincode, location, temperature, humidity, description)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    pincode,
                    data["name"],
                    data["main"]["temp"],
                    data["main"]["humidity"],
                    data["weather"][0]["description"]
                ))
            return True
        except sqlite3.Error as e:
            print(f"Database error: {str(e)}")
            return False

    def get_weather_history(self, pincode):
        """Get weather history for a pincode"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                query = '''
                    SELECT timestamp, temperature, humidity, description
                    FROM weather_records
                    WHERE pincode = ?
                    ORDER BY timestamp DESC
                    LIMIT 5
                '''
                return pd.read_sql_query(query, conn, params=(pincode,))
        except sqlite3.Error as e:
            print(f"Database error: {str(e)}")
            return pd.DataFrame()

class WeatherApp:
    """Main weather application"""

    def __init__(self, api_key):
        """Initialize WeatherApp with API key"""
        self.api = WeatherAPI(api_key)
        self.db = WeatherDatabase()

        # Create data directory if it doesn't exist
        Path("weather_data").mkdir(exist_ok=True)

    def save_to_json(self, data, pincode):
        """Save weather data to JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"weather_data/weather_{pincode}_{timestamp}.json"

        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Data saved to {filename}")
        except IOError as e:
            print(f"Error saving JSON file: {str(e)}")

    def display_weather(self, data):
        """Display current weather information"""
        print("\n=== Current Weather ===")
        print(f"Location: {data['name']}")
        print(f"Temperature: {data['main']['temp']}°C")
        print(f"Humidity: {data['main']['humidity']}%")
        print(f"Weather: {data['weather'][0]['description'].capitalize()}")
        print(f"Wind Speed: {data['wind']['speed']} m/s")
        print("=====================\n")

    def display_history(self, history_df):
        """Display weather history"""
        if history_df.empty:
            print("\nNo weather history available for this pincode")
            return

        print("\n=== Weather History ===")
        for _, row in history_df.iterrows():
            print(f"\nDate: {row['timestamp']}")
            print(f"Temperature: {row['temperature']}°C")
            print(f"Humidity: {row['humidity']}%")
            print(f"Conditions: {row['description']}")
        print("=====================\n")

    def run(self):
        """Run the main application loop"""
        while True:
            print("\nWeather Information System")
            print("1. Get current weather")
            print("2. View weather history")
            print("3. Exit")

            choice = input("\nEnter your choice (1-3): ")

            if choice == "1":
                pincode = input("Enter pincode: ")

                # Validate pincode
                if not pincode.isdigit() or len(pincode) != 6:
                    print("Error: Please enter a valid 6-digit pincode")
                    continue

                data = self.api.fetch_weather(pincode)

                if data:
                    self.display_weather(data)
                    self.save_to_json(data, pincode)
                    if self.db.save_weather_data(pincode, data):
                        print("Weather data saved to database")

            elif choice == "2":
                pincode = input("Enter pincode: ")
                history = self.db.get_weather_history(pincode)
                self.display_history(history)

            elif choice == "3":
                print("\nThank you for using the Weather Information System!")
                break

            else:
                print("\nInvalid choice. Please try again.")

def main():
    # Your OpenWeatherMap API key
    API_KEY = "69f7c28f70a606d634336bf70f9d0e5b"

    # Create and run the weather app
    app = WeatherApp(API_KEY)
    app.run()

if __name__ == "__main__":
    main()
    