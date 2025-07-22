# E-commerce Data Pipeline

A comprehensive data engineering project demonstrating multi-source data integration, automated processing, and analytics-ready data warehousing for e-commerce business intelligence.

## 🎯 Project Overview

This project builds a complete data pipeline that ingests, processes, and analyzes e-commerce data from multiple sources. It demonstrates real-world data engineering skills including data generation, API integration, web scraping, database design, and automated workflows.

### Business Problem Solved
- **Customer Analytics**: Track customer lifetime value, segmentation, and behavior
- **Inventory Management**: Monitor stock levels and demand patterns  
- **Sales Performance**: Analyze revenue trends and product performance
- **Market Intelligence**: Gather competitor pricing and social sentiment data

## 🏗️ Architecture

```
Data Sources → Python Processing → PostgreSQL → Analytics & ML
     ↓              ↓                 ↓           ↓
• CSV Files    • Data Cleaning    • Normalized   • Business Intelligence
• APIs         • Validation       • Tables       • Predictive Models
• Web Scraping • Transformation   • Indexes      • Dashboards
```

## 📊 Data Sources

### Internal Data Generation
- **Customers**: 1,000 realistic customer profiles with demographics and segments
- **Products**: 500 products across 6 categories with pricing and inventory
- **Transactions**: 5,000 sales transactions with payment and shipping details

### External Data Integration
- **Weather API**: Historical weather data affecting seasonal sales
- **Economic Indicators**: Unemployment, inflation, consumer confidence data
- **Social Media**: Reddit mentions and sentiment analysis
- **Competitor Data**: Product pricing and review scraping

### Sample Data Schema
```sql
-- Customer table structure
customers (customer_id, first_name, last_name, email, customer_segment, lifetime_value, ...)

-- Product table structure  
products (product_id, name, category, brand, price, stock_quantity, rating, ...)

-- Transaction table structure
transactions (transaction_id, customer_id, product_id, quantity, total_amount, transaction_date, ...)
```

## 🛠️ Technology Stack

- **Python**: Core data processing and automation
- **PostgreSQL**: Data warehouse and analytics database
- **pandas**: Data manipulation and cleaning
- **Faker**: Realistic test data generation
- **Beautiful Soup + Selenium**: Web scraping
- **psycopg2 + SQLAlchemy**: Database connectivity
- **APIs**: REST API integration (Weather, Economic data)

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/ngoclinhnguyen-2806/ecommerce-data-pipeline.git
   cd ecommerce-data-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up PostgreSQL**
   ```bash
   # Create database
   createdb ecommerce_db
   
   # Or use psql
   psql -U postgres -c "CREATE DATABASE ecommerce_db;"
   ```

4. **Configure database connection**
   ```python
   # Update password in basic_csv_loader.py
   password="your_postgresql_password"
   ```

### Running the Pipeline

1. **Generate sample data**
   ```bash
   python ecommerce_data_setup.py
   ```

2. **Load data into PostgreSQL**
   ```bash
   python basic_csv_loader.py
   ```

3. **Verify data loading**
   ```bash
   psql -U postgres -d ecommerce_db -c "SELECT COUNT(*) FROM customers;"
   ```

## 📈 Data Pipeline Features

### Data Quality & Validation
- Automated data type detection and conversion
- Missing value handling and data cleaning
- Duplicate record identification
- Data consistency checks across tables

### Database Design
- Normalized schema with proper relationships
- Optimized indexes for query performance
- Foreign key constraints for data integrity
- Scalable table design for growing datasets

### Analytics Capabilities
- Customer segmentation and lifetime value analysis
- Product performance and category trends
- Sales analytics and revenue reporting
- Time-series analysis for demand forecasting

## 📊 Sample Analytics Queries

### Customer Analytics
```sql
-- Top customers by revenue
SELECT first_name, last_name, customer_segment, 
       SUM(total_amount) as total_revenue
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, first_name, last_name, customer_segment
ORDER BY total_revenue DESC
LIMIT 10;
```

### Product Performance
```sql
-- Best selling products by category
SELECT p.category, p.name, 
       COUNT(t.transaction_id) as times_sold,
       SUM(t.total_amount) as revenue
FROM products p
JOIN transactions t ON p.product_id = t.product_id
GROUP BY p.category, p.name
ORDER BY revenue DESC;
```

### Business Intelligence
```sql
-- Monthly sales trends
SELECT DATE_TRUNC('month', transaction_date) as month,
       COUNT(*) as transaction_count,
       SUM(total_amount) as monthly_revenue,
       AVG(total_amount) as avg_order_value
FROM transactions
GROUP BY month
ORDER BY month;
```

## 🔍 Data Insights Generated

- **Customer Segments**: Premium (10%), Regular (70%), Budget (20%)
- **Top Categories**: Electronics, Clothing, Books generate 60% of revenue
- **Seasonal Patterns**: 15% revenue increase during holiday periods
- **Customer Behavior**: Average order value varies by segment (Premium: $125, Regular: $85, Budget: $45)

## 🚀 Future Enhancements

### Planned Features
- [ ] **Apache Airflow**: Workflow orchestration and scheduling
- [ ] **Machine Learning**: Churn prediction, demand forecasting, recommendation engine
- [ ] **Docker Containerization**: Easy deployment and scaling
- [ ] **Cloud Deployment**: AWS/GCP integration with managed services
- [ ] **Dashboard Creation**: Interactive visualizations with Streamlit
- [ ] **Data Lineage**: Track data flow and transformations
- [ ] **Monitoring & Alerting**: Pipeline health and performance monitoring

### Advanced Analytics
- Customer lifetime value prediction
- Demand forecasting with seasonal adjustment
- Inventory optimization

## 📁 Project Structure

```
ecommerce-data-pipeline/
├── data/
│   ├── raw/internal/          # Generated CSV files
│   ├── raw/external/          # API and scraped data
│   └── processed/             # Cleaned data
├── ecommerce_data_setup.py    # Main data generation script
├── basic_csv_loader.py        # PostgreSQL data loader
├── web_scraper.py             # Web scraping module
├── config.py                  # Configuration settings
├── requirements.txt           # Python dependencies
├── .env.example              # Environment variables template
└── README.md                 # Project documentation
```

## 🎯 Skills Demonstrated

### Data Engineering
- **ETL Pipeline Development**: Extract, Transform, Load processes
- **Database Design**: Normalized schemas and performance optimization
- **Data Integration**: Multiple source types (CSV, API, web scraping)

### Technical Skills
- **Python Development**: Object-oriented programming, error handling
- **SQL Proficiency**: Complex queries, joins, aggregations
- **API Integration**: REST API consumption and rate limiting
- **Web Scraping**: BeautifulSoup, Selenium for data extraction
- **Database Administration**: PostgreSQL setup, user management, indexing

### Business Intelligence
- **Analytics**: Customer segmentation, product performance analysis
- **Reporting**: Automated data summaries and insights
- **Data Modeling**: Business logic implementation in database design

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---
