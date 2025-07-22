# config.py
# Configuration file for the e-commerce data pipeline

import os
from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class APIConfig:
    """Configuration for external APIs"""
    openweather_api_key: Optional[str] = None
    fred_api_key: Optional[str] = None
    twitter_bearer_token: Optional[str] = None
    news_api_key: Optional[str] = None
    
    @classmethod
    def from_env(cls):
        """Load API keys from environment variables"""
        return cls(
            openweather_api_key=os.getenv('OPENWEATHER_API_KEY'),
            fred_api_key=os.getenv('FRED_API_KEY'),
            twitter_bearer_token=os.getenv('TWITTER_BEARER_TOKEN'),
            news_api_key=os.getenv('NEWS_API_KEY')
        )

@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "ecommerce_db"
    username: str = "postgres"
    password: str = "password"
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class ScrapingConfig:
    """Web scraping configuration"""
    headless: bool = True
    delay_range: tuple = (1, 3)
    max_retries: int = 3
    timeout: int = 30
    user_agents: List[str] = None
    
    def __post_init__(self):
        if self.user_agents is None:
            self.user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            ]

@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    output_dir: str = "data"
    num_customers: int = 1000
    num_products: int = 500
    num_transactions: int = 5000
    cities_for_weather: List[str] = None
    social_keywords: List[str] = None
    
    def __post_init__(self):
        if self.cities_for_weather is None:
            self.cities_for_weather = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
        
        if self.social_keywords is None:
            self.social_keywords = ['ecommerce', 'online shopping', 'retail trends', 'customer experience', 'digital commerce']

# Load configuration
API_CONFIG = APIConfig.from_env()
DB_CONFIG = DatabaseConfig()
SCRAPING_CONFIG = ScrapingConfig()
PIPELINE_CONFIG = PipelineConfig()