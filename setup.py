# setup.py
# Setup script to initialize the data pipeline environment

import os
import sys
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineSetup:
    """Setup the e-commerce data pipeline environment"""
    
    def __init__(self):
        self.project_root = Path.cwd()
        self.required_dirs = [
            "data/raw/internal",
            "data/raw/external", 
            "data/processed",
            "data/staging",
            "logs",
            "scripts",
            "notebooks",
            "tests",
            "config"
        ]
        
    def create_directory_structure(self):
        """Create the required directory structure"""
        logger.info("Creating directory structure...")
        
        for directory in self.required_dirs:
            dir_path = self.project_root / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created: {directory}")
            
        # Create .gitkeep files for empty directories
        for directory in self.required_dirs:
            gitkeep_path = self.project_root / directory / ".gitkeep"
            if not any((self.project_root / directory).iterdir()):
                gitkeep_path.touch()
    
    def install_requirements(self):
        """Install Python requirements"""
        logger.info("Installing Python requirements...")
        
        requirements_content = """pandas>=1.5.0
numpy>=1.21.0
faker>=18.0.0
requests>=2.28.0
beautifulsoup4>=4.11.0
selenium>=4.8.0
scrapy>=2.8.0
lxml>=4.9.0
python-dotenv>=0.19.0
jsonschema>=4.0.0
great-expectations>=0.15.0
psycopg2-binary>=2.9.0
pymongo>=4.0.0
sqlalchemy>=1.4.0
boto3>=1.26.0
prometheus-client>=0.16.0
structlog>=22.0.0
pytest>=7.0.0
pytest-mock>=3.10.0
jupyter>=1.0.0
matplotlib>=3.5.0
seaborn>=0.11.0
plotly>=5.0.0"""
        
        # Write requirements.txt
        requirements_path = self.project_root / "requirements.txt"
        requirements_path.write_text(requirements_content)
        
        # Install requirements
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
            logger.info("Requirements installed successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install requirements: {e}")
            return False
        
        return True
    
    def create_env_file(self):
        """Create .env file from template"""
        logger.info("Creating .env file...")
        
        env_content = """# E-commerce Data Pipeline Environment Variables

# Weather API (Get free key from: https://openweathermap.org/api)
OPENWEATHER_API_KEY=

# Economic Data API (Get free key from: https://fred.stlouisfed.org/docs/api/)
FRED_API_KEY=

# Twitter API (Get free key from: https://developer.twitter.com/)
TWITTER_BEARER_TOKEN=

# News API (Get free key from: https://newsapi.org/)
NEWS_API_KEY=

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_db
DB_USER=postgres
DB_PASSWORD=password

# AWS Configuration (Optional - for cloud deployment)
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-west-2

# Pipeline Configuration
PIPELINE_OUTPUT_DIR=data
PIPELINE_LOG_LEVEL=INFO"""

        env_path = self.project_root / ".env"
        if not env_path.exists():
            env_path.write_text(env_content)
            logger.info("Created .env file - please fill in your API keys")
        else:
            logger.info(".env file already exists")
    
    def create_gitignore(self):
        """Create .gitignore file"""
        logger.info("Creating .gitignore file...")
        
        gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environment
venv/
env/
ENV/

# Environment Variables
.env

# Data files
data/
*.csv
*.json
*.parquet
*.xlsx

# Logs
logs/
*.log

# Jupyter Notebooks
.ipynb_checkpoints

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Chrome Driver
chromedriver*

# Scrapy
.scrapy

# Database
*.db
*.sqlite3"""

        gitignore_path = self.project_root / ".gitignore"
        if not gitignore_path.exists():
            gitignore_path.write_text(gitignore_content)
            logger.info("Created .gitignore file")
    
    def create_readme(self):
        """Create README.md file"""
        logger.info("Creating README.md file...")
        
        readme_content = """# E-commerce Data Pipeline

A comprehensive data pipeline project showcasing multiple data source integration for e-commerce analytics.

## Features

- **Multi-source data integration**: APIs, web scraping, generated datasets
- **Real-time and batch processing**: Handles both streaming and batch data
- **Data quality validation**: Automated data quality checks
- **Scalable architecture**: Designed for cloud deployment
- **Comprehensive analytics**: Customer analytics, inventory optimization, fraud detection

## Data Sources

### Internal Data
- Customer profiles and transactions
- Product catalog and inventory
- Order and shipping data

### External APIs
- Weather data (OpenWeatherMap)
- Economic indicators (FRED API)
- Social media sentiment (Twitter API)
- News and trends (News API)

### Web Scraping
- Competitor pricing data
- Product reviews and ratings
- Social media mentions

## Quick Start

1. **Setup Environment**
   ```bash
   python setup.py
   pip install -r requirements.txt
   ```

2. **Configure API Keys**
   - Copy `.env.example` to `.env`
   - Fill in your API keys (see API Keys section)

3. **Run Data Generation**
   ```bash
   python ecommerce_data_setup.py
   ```

4. **Run Web Scraping**
   ```bash
   python web_scraper.py
   ```

## Project Structure

```
ecommerce-data-pipeline/
├── data/
│   ├── raw/
│   │   ├── internal/          # Generated e-commerce data
│   │   └── external/          # API and scraped data
│   ├── processed/             # Cleaned and transformed data
│   └── staging/               # Temporary processing data
├── scripts/                   # Data processing scripts
├── notebooks/                 # Jupyter notebooks for analysis
├── tests/                     # Unit tests
├── config/                    # Configuration files
└── logs/                      # Application logs
```

## API Keys

Get free API keys from:

- **OpenWeatherMap**: https://openweathermap.org/api
- **FRED Economic Data**: https://fred.stlouisfed.org/docs/api/
- **Twitter Developer**: https://developer.twitter.com/
- **News API**: https://newsapi.org/

## Technologies Used

- **Python**: Core language
- **Pandas**: Data manipulation
- **Requests/BeautifulSoup**: Web scraping
- **Selenium**: Dynamic content scraping
- **PostgreSQL**: Data storage
- **Apache Airflow**: Workflow orchestration
- **Docker**: Containerization
- **AWS/GCP**: Cloud deployment

## Analytics Capabilities

1. **Customer Analytics**
   - Customer lifetime value
   - Segmentation and behavior analysis
   - Churn prediction

2. **Inventory Optimization**
   - Demand forecasting
   - Stock level optimization
   - Seasonal trend analysis

3. **Market Intelligence**
   - Competitor pricing analysis
   - Social sentiment tracking
   - Market trend identification

## Development

### Running Tests
```bash
pytest tests/
```

### Adding New Data Sources
1. Create new integration in `data_sources/`
2. Add configuration to `config.py`
3. Update pipeline orchestration
4. Add tests

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License - see LICENSE file for details
"""

        readme_path = self.project_root / "README.md"
        if not readme_path.exists():
            readme_path.write_text(readme_content)
            logger.info("Created README.md file")
    
    def create_sample_scripts(self):
        """Create sample processing scripts"""
        logger.info("Creating sample scripts...")
        
        # Data validation script
        validation_script = """#!/usr/bin/env python3
# data_validation.py
# Data quality validation script

import pandas as pd
import numpy as np
from pathlib import Path
import logging

def validate_customers(df):
    \"\"\"Validate customer data quality\"\"\"
    issues = []
    
    # Check for missing values
    if df.isnull().any().any():
        issues.append("Missing values found in customer data")
    
    # Check email format
    email_pattern = r'^[\\w\\.-]+@[\\w\\.-]+\\.\\w+
    invalid_emails = ~df['email'].str.match(email_pattern, na=False)
    if invalid_emails.any():
        issues.append(f"{invalid_emails.sum()} invalid email addresses")
    
    # Check for duplicate customers
    duplicates = df.duplicated(subset=['email']).sum()
    if duplicates > 0:
        issues.append(f"{duplicates} duplicate customers found")
    
    return issues

def validate_transactions(df):
    \"\"\"Validate transaction data quality\"\"\"
    issues = []
    
    # Check for negative amounts
    negative_amounts = (df['total_amount'] < 0).sum()
    if negative_amounts > 0:
        issues.append(f"{negative_amounts} negative transaction amounts")
    
    # Check for future dates
    future_dates = (pd.to_datetime(df['transaction_date']) > pd.Timestamp.now()).sum()
    if future_dates > 0:
        issues.append(f"{future_dates} future transaction dates")
    
    return issues

if __name__ == "__main__":
    # Run validation on generated data
    data_dir = Path("data/raw/internal")
    
    customers = pd.read_csv(data_dir / "customers.csv")
    transactions = pd.read_csv(data_dir / "transactions.csv")
    
    customer_issues = validate_customers(customers)
    transaction_issues = validate_transactions(transactions)
    
    print("Data Validation Results:")
    print("=" * 50)
    
    if customer_issues:
        print("Customer Data Issues:")
        for issue in customer_issues:
            print(f"  - {issue}")
    else:
        print("✓ Customer data validation passed")
    
    if transaction_issues:
        print("Transaction Data Issues:")
        for issue in transaction_issues:
            print(f"  - {issue}")
    else:
        print("✓ Transaction data validation passed")
"""
        
        scripts_dir = self.project_root / "scripts"
        validation_path = scripts_dir / "data_validation.py"
        validation_path.write_text(validation_script)
        
        # Analytics script
        analytics_script = """#!/usr/bin/env python3
# basic_analytics.py
# Basic analytics on the generated data

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

def customer_analytics(customers_df, transactions_df):
    \"\"\"Generate customer analytics\"\"\"
    print("Customer Analytics")
    print("=" * 50)
    
    # Customer segments
    segment_counts = customers_df['customer_segment'].value_counts()
    print(f"Customer Segments:\\n{segment_counts}\\n")
    
    # Top customers by transaction count
    customer_txns = transactions_df.groupby('customer_id').size().sort_values(ascending=False)
    print(f"Top 5 Customers by Transaction Count:\\n{customer_txns.head()}\\n")
    
    # Customer lifetime value distribution
    print(f"Customer Lifetime Value Stats:\\n{customers_df['lifetime_value'].describe()}\\n")

def product_analytics(products_df, transactions_df):
    \"\"\"Generate product analytics\"\"\"
    print("Product Analytics")
    print("=" * 50)
    
    # Top categories
    category_counts = products_df['category'].value_counts()
    print(f"Product Categories:\\n{category_counts}\\n")
    
    # Best selling products
    product_sales = transactions_df.groupby('product_id')['quantity'].sum().sort_values(ascending=False)
    print(f"Top 5 Products by Quantity Sold:\\n{product_sales.head()}\\n")
    
    # Revenue by category
    product_revenue = transactions_df.merge(products_df, on='product_id')
    category_revenue = product_revenue.groupby('category')['total_amount'].sum().sort_values(ascending=False)
    print(f"Revenue by Category:\\n{category_revenue}\\n")

if __name__ == "__main__":
    # Load data
    data_dir = Path("data/raw/internal")
    
    customers = pd.read_csv(data_dir / "customers.csv")
    products = pd.read_csv(data_dir / "products.csv")
    transactions = pd.read_csv(data_dir / "transactions.csv")
    
    # Run analytics
    customer_analytics(customers, transactions)
    product_analytics(products, transactions)
    
    print("Analytics completed! Check the output above for insights.")
"""
        
        analytics_path = scripts_dir / "basic_analytics.py"
        analytics_path.write_text(analytics_script)
        
        logger.info("Created sample scripts in scripts/ directory")
    
    def run_setup(self):
        """Run the complete setup process"""
        logger.info("Starting e-commerce data pipeline setup...")
        
        try:
            self.create_directory_structure()
            self.create_env_file()
            self.create_gitignore()
            self.create_readme()
            self.create_sample_scripts()
            
            logger.info("✓ Setup completed successfully!")
            logger.info("Next steps:")
            logger.info("1. Fill in your API keys in the .env file")
            logger.info("2. Run: python ecommerce_data_setup.py")
            logger.info("3. Run: python scripts/data_validation.py")
            logger.info("4. Run: python scripts/basic_analytics.py")
            
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False

if __name__ == "__main__":
    setup = PipelineSetup()
    setup.run_setup()