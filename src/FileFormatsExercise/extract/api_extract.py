import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup


def extract_weather_from_sinoptik(city: str = "sofia") -> pd.DataFrame:
    """
    Extracts weather data for a specified city from the Sinoptik website and returns it as a pandas DataFrame.

    """
    url = f"https://www.sinoptik.bg/sofia-bulgaria-100727011"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/58.0.3029.110 Safari/537.3"
        )
    }
    
    logging.info(f"Starting extraction of weather data from {url}")

    try:
        response = requests.get(url, headers=headers, timeout=10)
    except Exception as e:
        logging.error(f"Error fetching data from {url}: {e}")
        raise

    try:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        temp_node = soup.find('span', class_="wfCurrentTemp")
        feel_node = soup.find('span', class_="wfCurrentFeelTemp")

        temperature = temp_node.text.strip() if temp_node else None
        feels_like = feel_node.text.strip() if feel_node else None

        if temperature is None or feels_like is None:
            raise ValueError("Could not find temperature data on the page")
    
    except Exception as e:
        logging.error(f"Error parsing weather data from {url}: {e}")
        raise

    df = pd.DataFrame({
        'city': [city.capitalize()],
        'temperature': [temperature],
        'feels_like': [feels_like]
    })

    logging.info(f"Successfully extracted weather data for {city}:{df.to_dict(orient='records')}")
    
    return df

