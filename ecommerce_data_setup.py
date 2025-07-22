# E-commerce Data Pipeline - Data Sources Setup
# This script sets up multiple data sources for your e-commerce data pipeline

import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import random
from faker import Faker
import time
import os
from typing import Dict, List, Any
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceDataGenerator:
    """Generate realistic e-commerce data for the pipeline"""
    
    def __init__(self, seed=42):
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        self.categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Beauty']
        self.brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
        
    def generate_customers(self, num_customers=1000) -> pd.DataFrame:
        """Generate customer data"""
        customers = []
        for i in range(num_customers):
            customers.append({
                'customer_id': f'CUST_{i+1:06d}',
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'address': self.fake.address().replace('\n', ', '),
                'city': self.fake.city(),
                'state': self.fake.state(),
                'zip_code': self.fake.zipcode(),
                'country': 'USA',
                'date_joined': self.fake.date_between(start_date='-2y', end_date='today'),
                'customer_segment': random.choice(['Premium', 'Regular', 'Budget']),
                'lifetime_value': round(random.uniform(100, 5000), 2)
            })
        return pd.DataFrame(customers)
    
    def generate_products(self, num_products=500) -> pd.DataFrame:
        """Generate product catalog"""
        products = []
        for i in range(num_products):
            category = random.choice(self.categories)
            base_price = round(random.uniform(10, 500), 2)
            products.append({
                'product_id': f'PROD_{i+1:06d}',
                'name': f'{self.fake.catch_phrase()} {category.split()[0]}',
                'description': self.fake.text(max_nb_chars=200),
                'category': category,
                'brand': random.choice(self.brands),
                'price': base_price,
                'cost': round(base_price * 0.6, 2),
                'weight': round(random.uniform(0.1, 5.0), 2),
                'dimensions': f"{random.randint(5,30)}x{random.randint(5,30)}x{random.randint(5,30)}",
                'stock_quantity': random.randint(0, 1000),
                'rating': round(random.uniform(1, 5), 1),
                'reviews_count': random.randint(0, 500),
                'date_added': self.fake.date_between(start_date='-1y', end_date='today')
            })
        return pd.DataFrame(products)
    
    def generate_transactions(self, customers_df, products_df, num_transactions=5000) -> pd.DataFrame:
        """Generate transaction data"""
        transactions = []
        for i in range(num_transactions):
            customer = customers_df.sample(1).iloc[0]
            product = products_df.sample(1).iloc[0]
            quantity = random.randint(1, 5)
            
            # Apply customer segment discount
            discount_rate = {'Premium': 0.1, 'Regular': 0.05, 'Budget': 0.02}
            discount = discount_rate.get(customer['customer_segment'], 0)
            
            unit_price = product['price'] * (1 - discount)
            total_amount = round(unit_price * quantity, 2)
            
            transactions.append({
                'transaction_id': f'TXN_{i+1:08d}',
                'customer_id': customer['customer_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': round(unit_price, 2),
                'total_amount': total_amount,
                'discount_amount': round(product['price'] * quantity * discount, 2),
                'tax_amount': round(total_amount * 0.08, 2),
                'shipping_cost': round(random.uniform(0, 15), 2),
                'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
                'transaction_date': self.fake.date_time_between(start_date='-6m', end_date='now'),
                'order_status': random.choice(['Completed', 'Pending', 'Cancelled', 'Refunded']),
                'shipping_address': customer['address'],
                'channel': random.choice(['Website', 'Mobile App', 'In-Store', 'Phone'])
            })
        return pd.DataFrame(transactions)

class APIIntegrator:
    """Integrate with external APIs"""
    
    def __init__(self):
        self.base_delay = 1  # Rate limiting
        
    def get_fake_store_data(self) -> Dict[str, Any]:
        """Fetch data from Fake Store API"""
        base_url = "https://fakestoreapi.com"
        data = {}
        
        try:
            # Get products
            logger.info("Fetching products from Fake Store API...")
            response = requests.get(f"{base_url}/products")
            data['products'] = response.json()
            time.sleep(self.base_delay)
            
            # Get users
            logger.info("Fetching users from Fake Store API...")
            response = requests.get(f"{base_url}/users")
            data['users'] = response.json()
            time.sleep(self.base_delay)
            
            # Get carts
            logger.info("Fetching carts from Fake Store API...")
            response = requests.get(f"{base_url}/carts")
            data['carts'] = response.json()
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching Fake Store data: {e}")
            return {}
    
    def get_weather_data(self, api_key: str = None, cities: List[str] = None) -> pd.DataFrame:
        """Fetch weather data (requires API key)"""
        if not api_key:
            logger.warning("No weather API key provided. Generating mock weather data.")
            return self._generate_mock_weather_data(cities or ['New York', 'Los Angeles', 'Chicago'])
        
        # Real weather API implementation would go here
        weather_data = []
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        
        for city in cities or ['New York', 'Los Angeles', 'Chicago']:
            try:
                params = {'q': city, 'appid': api_key, 'units': 'metric'}
                response = requests.get(base_url, params=params)
                data = response.json()
                
                weather_data.append({
                    'city': city,
                    'temperature': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'weather_condition': data['weather'][0]['main'],
                    'timestamp': datetime.now()
                })
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.error(f"Error fetching weather for {city}: {e}")
        
        return pd.DataFrame(weather_data)
    
    def _generate_mock_weather_data(self, cities: List[str]) -> pd.DataFrame:
        """Generate mock weather data for testing"""
        weather_data = []
        conditions = ['Clear', 'Clouds', 'Rain', 'Snow', 'Thunderstorm']
        
        for city in cities:
            for days_back in range(30):  # Last 30 days
                date = datetime.now() - timedelta(days=days_back)
                weather_data.append({
                    'city': city,
                    'temperature': round(random.uniform(-10, 35), 1),
                    'humidity': random.randint(20, 100),
                    'weather_condition': random.choice(conditions),
                    'timestamp': date
                })
        
        return pd.DataFrame(weather_data)
    
    def get_economic_data(self) -> pd.DataFrame:
        """Get economic indicators (mock data)"""
        # In production, this would use FRED API or similar
        economic_data = []
        
        for days_back in range(365):  # Last year
            date = datetime.now() - timedelta(days=days_back)
            economic_data.append({
                'date': date.date(),
                'unemployment_rate': round(random.uniform(3.5, 8.0), 1),
                'inflation_rate': round(random.uniform(0.5, 6.0), 1),
                'consumer_confidence': round(random.uniform(80, 130), 1),
                'gdp_growth': round(random.uniform(-2.0, 5.0), 1)
            })
        
        return pd.DataFrame(economic_data)

class DataPipeline:
    """Main data pipeline orchestrator"""
    
    def __init__(self, output_dir: str = "data"):
        self.output_dir = output_dir
        self.generator = EcommerceDataGenerator()
        self.api_integrator = APIIntegrator()
        self._setup_directories()
    
    def _setup_directories(self):
        """Create directory structure"""
        directories = [
            f"{self.output_dir}/raw/internal",
            f"{self.output_dir}/raw/external",
            f"{self.output_dir}/processed",
            f"{self.output_dir}/staging"
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def generate_internal_data(self):
        """Generate internal e-commerce data"""
        logger.info("Generating internal e-commerce data...")
        
        # Generate base data
        customers = self.generator.generate_customers(1000)
        products = self.generator.generate_products(500)
        transactions = self.generator.generate_transactions(customers, products, 5000)
        
        # Save to CSV
        customers.to_csv(f"{self.output_dir}/raw/internal/customers.csv", index=False)
        products.to_csv(f"{self.output_dir}/raw/internal/products.csv", index=False)
        transactions.to_csv(f"{self.output_dir}/raw/internal/transactions.csv", index=False)
        
        logger.info(f"Generated {len(customers)} customers, {len(products)} products, {len(transactions)} transactions")
        
        return customers, products, transactions
    
    def fetch_external_data(self, weather_api_key: str = None):
        """Fetch external API data"""
        logger.info("Fetching external data...")
        
        # Fake Store API
        fake_store_data = self.api_integrator.get_fake_store_data()
        if fake_store_data:
            with open(f"{self.output_dir}/raw/external/fake_store_data.json", 'w') as f:
                json.dump(fake_store_data, f, indent=2, default=str)
        
        # Weather data
        weather_data = self.api_integrator.get_weather_data(weather_api_key)
        weather_data.to_csv(f"{self.output_dir}/raw/external/weather_data.csv", index=False)
        
        # Economic data
        economic_data = self.api_integrator.get_economic_data()
        economic_data.to_csv(f"{self.output_dir}/raw/external/economic_data.csv", index=False)
        
        logger.info("External data fetched successfully")
    
    def run_full_pipeline(self, weather_api_key: str = None):
        """Run the complete data generation pipeline"""
        logger.info("Starting full data pipeline...")
        
        # Generate internal data
        customers, products, transactions = self.generate_internal_data()
        
        # Fetch external data
        self.fetch_external_data(weather_api_key)
        
        # Create a summary report
        summary = {
            'pipeline_run_date': datetime.now().isoformat(),
            'customers_generated': len(customers),
            'products_generated': len(products),
            'transactions_generated': len(transactions),
            'data_sources': [
                'Internal: customers, products, transactions',
                'External: Fake Store API, Weather API, Economic indicators'
            ]
        }
        
        with open(f"{self.output_dir}/pipeline_summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info("Pipeline completed successfully!")
        logger.info(f"Data saved to: {self.output_dir}")
        
        return summary

if __name__ == "__main__":
    # Initialize and run pipeline
    pipeline = DataPipeline()
    
    # Run with optional weather API key
    # Get free API key from: https://openweathermap.org/api
    weather_api_key = '1f259dc4e7eedae9e556d962a63deaf6'  # Replace with your API key
    
    summary = pipeline.run_full_pipeline(weather_api_key)
    print(f"\nPipeline Summary:")
    print(json.dumps(summary, indent=2))