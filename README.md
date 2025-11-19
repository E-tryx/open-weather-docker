# Kenya Weather Data Analytics Platform

A comprehensive real-time weather monitoring and analytics platform for Kenyan cities, featuring Kafka streaming, PostgreSQL storage, and Grafana visualization.

## 🌟 Overview

This project provides a complete end-to-end solution for collecting, processing, storing, and visualizing weather data from multiple Kenyan cities. The platform combines real-time data streaming with historical analysis capabilities.

## 🏗️ Architecture

```
OpenWeatherMap API → Kafka Producers → Kafka Topics → Kafka Consumers → PostgreSQL → Grafana Dashboards
      ↓                    ↓               ↓              ↓              ↓            ↓
 Real-time Data      Data Ingestion    Message Queue  Data Processing  Data Storage  Visualization
```

## 📁 Project Structure

```
├── producers/
│   ├── producer.py              # Real-time weather data producer
│   ├── historical_producer.py   # Historical data generator
│   └── data_gen.py             # Data generation functions
├── consumers/
│   ├── consumer.py             # Main consumer with batch processing
│   └── consumer_confluent.py   # Alternative consumer implementation
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── datasource.yml  # PostgreSQL datasource config
│   │   └── dashboards/
│   │       └── dashboard.yml   # Dashboard provisioning
│   ├── dashboards/
│   │   ├── weather-overview.json
│   │   ├── rainfall-analysis.json
│   │   ├── annual-rainfall.json
│   │   └── historiacal.json
├── scripts/
│   ├── setup_grafana.sh        # Grafana setup script
│   └── SqlQuery.md            # Comprehensive SQL queries
├── docker-compose.yml         # Infrastructure setup
├── requirements.txt           # Python dependencies
└── .env.example              # Environment template
```

## 🚀 Quick Start

### 1. Prerequisites

- Docker & Docker Compose
- Python 3.8+
- OpenWeatherMap API key

### 2. Environment Setup

```bash
# Copy environment template
cp .env.example .env

# Edit with your credentials
nano .env
```

**Required Environment Variables:**
```env
# OpenWeatherMap API
OPENWEATHER_API_KEY=your_openweather_api_key

# Aiven PostgreSQL Configuration
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=25000
POSTGRES_DB=your_database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_SSL_MODE=require

# Grafana Configuration
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=changeme123
```

### 3. Start Infrastructure

```bash
# Start all services
docker compose up -d

# Check services
docker compose ps
```

### 4. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 5. Setup Grafana

```bash
# Make script executable
chmod +x setup_grafana.sh

# Run setup script
./setup_grafana.sh
```

### 6. Run the System

**Start Real-time Data Production:**
```bash
python producer.py
```

**Start Data Consumption:**
```bash
python consumer.py --mode current --batch-size 100
```

**Generate Historical Data:**
```bash
python historical_producer.py
```

### 7. Access Services

- **Grafana Dashboard**: http://localhost:3000 (admin/admin123)
- **Kafdrop UI**: http://localhost:9000
- **Kafka Broker**: localhost:9094

## 🔧 Core Components

### 1. Data Producers

**Real-time Producer (`producer.py`)**
- Fetches live weather data from OpenWeatherMap API
- Supports multiple Kenyan cities
- Produces to Kafka topics in real-time

**Historical Producer (`historical_producer.py`)**
- Generates realistic historical weather data (2014-present)
- Simulates Kenyan climate patterns
- Produces to historical Kafka topics

**Supported Cities:**
- Nairobi (-1.292, 36.821)
- Eldoret (0.514, 35.269) 
- Biretwo (0.519, 35.270)
- Naiberi (0.553, 35.357)
- Annex Eldoret (0.520, 35.280)

### 2. Data Consumers

**Main Consumer (`consumer.py`)**
- Batch processing with configurable batch size
- Automatic schema evolution
- Retry logic and error handling
- Multiple consumption modes (current, historical, both, auto)

**Features:**
- Batch insertion (100 records default)
- Automatic table creation and schema updates
- Connection retry logic
- Real-time progress monitoring

### 3. Data Storage

**PostgreSQL Schema:**
```sql
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(10),
    temperature DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    humidity INTEGER,
    pressure INTEGER,
    weather_main VARCHAR(100),
    weather_description VARCHAR(100),
    wind_speed DECIMAL(5,2),
    rainfall_1h DECIMAL(5,2),
    rainfall_3h DECIMAL(5,2),
    rain_probability DECIMAL(5,2),
    month INTEGER,
    year INTEGER,
    data_type VARCHAR(50),
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 4. Visualization (Grafana)

**Available Dashboards:**
- **Weather Overview**: Real-time temperature, humidity, and conditions
- **Rainfall Analysis**: Precipitation patterns and intensity
- **Annual Rainfall**: Monthly rainfall comparison across cities
- **Historical Analysis**: Long-term climate trends and patterns

## ⚙️ Configuration

### Kafka Topics

**Real-time Topics:**
- `biretwo`, `eldoret`, `naiberi`, `annex_eldoret`, `nairobi`

**Historical Topics:**
- `{city}_historical` (e.g., `nairobi_historical`)

### Consumer Modes

```bash
# Current data only
python consumer.py --mode current

# Historical data only  
python consumer.py --mode historical

# Both current and historical
python consumer.py --mode both

# Auto-detect available topics
python consumer.py --mode auto

# Custom batch settings
python consumer.py --batch-size 50 --batch-timeout 30
```

### Data Metrics Collected

- **Temperature** (°C): Current and feels-like
- **Humidity** (%): Relative humidity levels
- **Pressure** (hPa): Atmospheric pressure
- **Wind Speed** (m/s): Wind velocity
- **Rainfall** (mm): 1-hour and 3-hour precipitation
- **Weather Conditions**: Main weather and descriptions
- **Probability**: Rain probability percentages

## 📊 Analytics & Queries

### Comprehensive SQL Analysis

The project includes extensive SQL queries for:

- **Monthly temperature analysis** with averages, min, max
- **Rainfall patterns** and intensity categorization
- **Seasonal analysis** specific to Kenyan climate
- **Weather condition frequency** analysis
- **City comparison** and trend analysis
- **Climate correlation** studies

See `SqlQuery.md` for complete query list.

### Kenyan Climate Patterns

The system models three distinct Kenyan seasons:
- **Dry Season** (Jan-Feb, Jun-Sep): Higher temperatures, low rainfall
- **Long Rains** (Mar-May): Moderate temperatures, high rainfall
- **Short Rains** (Oct-Dec): Variable temperatures, moderate rainfall

## 🛠️ Development

### Adding New Cities

1. Add city coordinates to `data_gen.py`
2. Update consumer topic subscriptions
3. Regenerate historical data if needed

### Customizing Data Collection

```python
# Modify data generation in data_gen.py
def new_city():
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': YOUR_LATITUDE,
        'lon': YOUR_LONGITUDE,
        'appid': os.getenv('OPENWEATHER_API_KEY'),
        'units': 'metric'
    }
    # ... data processing
```

### Extending Schema

The consumer automatically handles schema evolution:
```python
# New columns are automatically added
ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS new_column DECIMAL(5,2);
```

## 📈 Monitoring & Operations

### Service Health

```bash
# Check all services
docker compose ps

# View logs
docker compose logs kafka
docker compose logs grafana

# Monitor Kafka topics
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9094
```

### Performance Monitoring

- **Kafka**: Monitor through Kafdrop UI
- **PostgreSQL**: Query performance via EXPLAIN ANALYZE
- **Grafana**: Built-in dashboard performance metrics

### Data Quality Checks

```sql
-- Check data completeness
SELECT city, COUNT(*) as records, MIN(timestamp), MAX(timestamp)
FROM weather_data 
GROUP BY city;

-- Validate data ranges
SELECT 
    MIN(temperature) as min_temp,
    MAX(temperature) as max_temp,
    AVG(humidity) as avg_humidity
FROM weather_data;
```

## 🐛 Troubleshooting

### Common Issues

**Kafka Connection Problems:**
```bash
# Check Kafka is running
docker compose ps kafka

# Test Kafka connection
python -c "from kafka import KafkaProducer; producer = KafkaProducer(bootstrap_servers='localhost:9094')"
```

**PostgreSQL Connection Issues:**
- Verify credentials in `.env`
- Check SSL certificate requirements
- Confirm network connectivity to Aiven

**Grafana Dashboard Errors:**
- Verify datasource configuration
- Check PostgreSQL table permissions
- Validate SQL query syntax

### Logs and Debugging

```bash
# Consumer logs with debug info
python consumer.py --mode current --batch-size 10

# Check Kafka topic messages
docker compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9094 \
    --topic nairobi \
    --from-beginning

# Database connection test
python -c "
import psycopg2
import os
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    sslmode=os.getenv('POSTGRES_SSL_MODE')
)
print('Connection successful')
"
```

### Reset Environment

```bash
# Stop and clean
docker compose down -v

# Restart fresh
docker compose up -d
```

## 🔄 Data Pipeline Flow

1. **Data Collection**: OpenWeatherMap API → Kafka Producers
2. **Stream Processing**: Kafka Topics → Consumer Batch Processing
3. **Storage**: PostgreSQL with optimized schema
4. **Visualization**: Grafana dashboards with real-time updates
5. **Analysis**: Comprehensive SQL queries for insights

## 📝 License

This project is designed for educational and analytical purposes. Please ensure compliance with OpenWeatherMap API terms of service.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/improvement`)
5. Create Pull Request

---

**Note**: Replace all placeholder credentials in `.env` with your actual API keys and database credentials. Ensure you have proper OpenWeatherMap API access and Aiven PostgreSQL service running.
