# rainfall_analysis.py
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

load_dotenv()

class RainfallAnalyzer:
    def __init__(self):
        self.conn = self.get_db_connection()
        self.month_names = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }
        
    def get_db_connection(self):
        """Establish connection to PostgreSQL database"""
        return psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            sslmode=os.getenv('POSTGRES_SSL_MODE')
        )
    
    def get_monthly_rainfall_data(self):
        """Get monthly rainfall data for all cities"""
        query = """
        SELECT 
            city,
            month,
            year,
            AVG(rainfall_1h * 24) as avg_daily_rainfall_mm,
            COUNT(*) as records,
            COUNT(CASE WHEN rainfall_1h > 0.2 THEN 1 END) as rainy_days,
            MAX(rainfall_1h) as max_hourly_rainfall
        FROM weather_data 
        WHERE data_type = 'historical_simulated'
        GROUP BY city, month, year
        ORDER BY city, year, month
        """
        
        return pd.read_sql(query, self.conn)
    
    def analyze_city_rainfall_patterns(self, city):
        """Analyze rainfall patterns for a specific city"""
        query = f"""
        SELECT 
            month,
            AVG(rainfall_1h * 24) as avg_daily_rainfall_mm,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rainfall_1h * 24) as median_daily_rainfall,
            STDDEV(rainfall_1h * 24) as rainfall_std_dev,
            COUNT(CASE WHEN rainfall_1h > 0.2 THEN 1 END) * 100.0 / COUNT(*) as rainy_days_percent,
            MAX(rainfall_1h * 24) as max_daily_rainfall
        FROM weather_data 
        WHERE data_type = 'historical_simulated' AND city = '{city}'
        GROUP BY month
        ORDER BY month
        """
        
        df = pd.read_sql(query, self.conn)
        df['month_name'] = df['month'].map(self.month_names)
        return df
    
    def generate_monthly_report(self):
        """Generate comprehensive monthly rainfall report"""
        cities = ['Nairobi', 'Eldoret', 'Biretwo', 'Naiberi', 'Annex Eldoret']
        
        print("=" * 80)
        print("KENYA RAINFALL ANALYSIS REPORT - MONTHLY PATTERNS")
        print("=" * 80)
        
        for city in cities:
            print(f"\n{'='*50}")
            print(f"RAINFALL ANALYSIS FOR {city.upper()}")
            print(f"{'='*50}")
            
            data = self.analyze_city_rainfall_patterns(city)
            
            # Display monthly statistics
            print(f"\nMonthly Rainfall Statistics (mm/day):")
            print("-" * 40)
            for _, row in data.iterrows():
                print(f"{row['month_name']:12} | Avg: {row['avg_daily_rainfall_mm']:5.1f} mm | "
                      f"Rainy Days: {row['rainy_days_percent']:4.1f}% | "
                      f"Max: {row['max_daily_rainfall']:5.1f} mm")
            
            # Identify seasons
            wettest_month = data.loc[data['avg_daily_rainfall_mm'].idxmax()]
            driest_month = data.loc[data['avg_daily_rainfall_mm'].idxmin()]
            
            print(f"\nKey Findings for {city}:")
            print(f"  • Wettest Month: {wettest_month['month_name']} ({wettest_month['avg_daily_rainfall_mm']:.1f} mm/day)")
            print(f"  • Driest Month: {driest_month['month_name']} ({driest_month['avg_daily_rainfall_mm']:.1f} mm/day)")
            print(f"  • Annual Rainfall: {data['avg_daily_rainfall_mm'].sum() * 30:.0f} mm (estimated)")
    
    def create_rainfall_visualizations(self):
        """Create rainfall visualization charts"""
        data = self.get_monthly_rainfall_data()
        
        # Set up the plotting style
        plt.style.use('seaborn-v0_8')
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Kenya Monthly Rainfall Analysis by Location', fontsize=16, fontweight='bold')
        
        # Plot 1: Monthly rainfall patterns by city
        plt.subplot(2, 2, 1)
        monthly_avg = data.groupby(['city', 'month'])['avg_daily_rainfall_mm'].mean().reset_index()
        
        for city in monthly_avg['city'].unique():
            city_data = monthly_avg[monthly_avg['city'] == city]
            plt.plot(city_data['month'], city_data['avg_daily_rainfall_mm'], 
                    marker='o', linewidth=2, label=city)
        
        plt.xlabel('Month')
        plt.ylabel('Average Daily Rainfall (mm)')
        plt.title('Monthly Rainfall Patterns')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Plot 2: Rainfall distribution by season
        plt.subplot(2, 2, 2)
        data['season'] = data['month'].apply(self._get_season)
        seasonal_rainfall = data.groupby(['city', 'season'])['avg_daily_rainfall_mm'].mean().unstack()
        seasonal_rainfall.plot(kind='bar', ax=plt.gca())
        plt.title('Average Rainfall by Season')
        plt.ylabel('Average Daily Rainfall (mm)')
        plt.xticks(rotation=45)
        plt.legend(title='Season')
        
        # Plot 3: Rainy days percentage
        plt.subplot(2, 2, 3)
        rainy_days_data = data.groupby(['city', 'month'])['rainy_days'].mean().reset_index()
        rainy_days_pivot = rainy_days_data.pivot(index='month', columns='city', values='rainy_days')
        sns.heatmap(rainy_days_pivot, annot=True, fmt='.0f', cmap='YlGnBu', ax=plt.gca())
        plt.title('Average Rainy Days per Month')
        
        # Plot 4: Annual rainfall comparison
        plt.subplot(2, 2, 4)
        annual_rainfall = data.groupby(['city', 'year'])['avg_daily_rainfall_mm'].mean().groupby('city').mean() * 365
        annual_rainfall.plot(kind='bar', color='skyblue')
        plt.title('Estimated Annual Rainfall by City')
        plt.ylabel('Annual Rainfall (mm)')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig('rainfall_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def _get_season(self, month):
        """Categorize month into Kenyan seasons"""
        if month in [3, 4, 5]:
            return 'Long Rains'
        elif month in [10, 11, 12]:
            return 'Short Rains'
        else:
            return 'Dry Season'
    
    def export_monthly_data(self):
        """Export monthly rainfall data to CSV for further analysis"""
        data = self.get_monthly_rainfall_data()
        data['month_name'] = data['month'].map(self.month_names)
        data['season'] = data['month'].apply(self._get_season)
        
        # Calculate additional metrics
        summary = data.groupby(['city', 'month', 'month_name', 'season']).agg({
            'avg_daily_rainfall_mm': 'mean',
            'rainy_days': 'mean',
            'max_hourly_rainfall': 'max'
        }).reset_index()
        
        summary['rainy_days_percent'] = (summary['rainy_days'] / 30) * 100  # Approximate days in month
        
        # Save to CSV
        summary.to_csv('monthly_rainfall_analysis.csv', index=False)
        print("Monthly rainfall data exported to 'monthly_rainfall_analysis.csv'")
        
        return summary

if __name__ == "__main__":
    analyzer = RainfallAnalyzer()
    
    # Generate comprehensive report
    analyzer.generate_monthly_report()
    
    # Create visualizations
    analyzer.create_rainfall_visualizations()
    
    # Export data
    analyzer.export_monthly_data()