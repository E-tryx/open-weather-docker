import requests, time
from datetime import datetime

def biretwo():
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': 0.519,
        'lon': 35.270,
        'appid': '0384de6d728525bffb441c520c59a46f',  # Replace with your OpenWeather API key
        'units': 'metric'
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    yield {
        "city": "Biretwo",
        "country": "KE",
        "temperature": data['main']['temp'],
        "feels_like": data['main']['feels_like'],
        "humidity": data['main']['humidity'],
        "pressure": data['main']['pressure'],
        "weather_main": data['weather'][0]['main'],
        "weather_description": data['weather'][0]['description'],
        "wind_speed": data['wind']['speed'],
        "timestamp": datetime.utcnow().isoformat()
    }

def eldoret():
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': 0.514,
        'lon': 35.269,
        'appid': '0384de6d728525bffb441c520c59a46f',  # Replace with your OpenWeather API key
        'units': 'metric'
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    yield {
        "city": "Eldoret",
        "country": "KE",
        "temperature": data['main']['temp'],
        "feels_like": data['main']['feels_like'],
        "humidity": data['main']['humidity'],
        "pressure": data['main']['pressure'],
        "weather_main": data['weather'][0]['main'],
        "weather_description": data['weather'][0]['description'],
        "wind_speed": data['wind']['speed'],
        "timestamp": datetime.utcnow().isoformat()
    }

def naiberi():
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': 0.553,
        'lon': 35.357,
        'appid': '0384de6d728525bffb441c520c59a46f',  # Replace with your OpenWeather API key
        'units': 'metric'
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    yield {
        "city": "Naiberi",
        "country": "KE",
        "temperature": data['main']['temp'],
        "feels_like": data['main']['feels_like'],
        "humidity": data['main']['humidity'],
        "pressure": data['main']['pressure'],
        "weather_main": data['weather'][0]['main'],
        "weather_description": data['weather'][0]['description'],
        "wind_speed": data['wind']['speed'],
        "timestamp": datetime.utcnow().isoformat()
    }

def annex_eldoret():
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': 0.520,
        'lon': 35.280,
        'appid': '0384de6d728525bffb441c520c59a46f',  # Replace with your OpenWeather API key
        'units': 'metric'
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    yield {
        "city": "Annex Eldoret",
        "country": "KE",
        "temperature": data['main']['temp'],
        "feels_like": data['main']['feels_like'],
        "humidity": data['main']['humidity'],
        "pressure": data['main']['pressure'],
        "weather_main": data['weather'][0]['main'],
        "weather_description": data['weather'][0]['description'],
        "wind_speed": data['wind']['speed'],
        "timestamp": datetime.utcnow().isoformat()
    }

def nairobi():
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': -1.292,
        'lon': 36.821,
        'appid': '0384de6d728525bffb441c520c59a46f',  # Replace with your OpenWeather API key
        'units': 'metric'
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    yield {
        "city": "Nairobi",
        "country": "KE",
        "temperature": data['main']['temp'],
        "feels_like": data['main']['feels_like'],
        "humidity": data['main']['humidity'],
        "pressure": data['main']['pressure'],
        "weather_main": data['weather'][0]['main'],
        "weather_description": data['weather'][0]['description'],
        "wind_speed": data['wind']['speed'],
        "timestamp": datetime.utcnow().isoformat()
    }