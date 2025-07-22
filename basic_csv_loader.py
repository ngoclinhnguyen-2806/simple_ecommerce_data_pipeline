# basic_csv_loader.py
# Simple, reliable CSV to PostgreSQL loader

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimplePostgreSQLLoader:
    def __init__(self, host="localhost", port="5432", database="postgres", 
                 user="postgres", password="postgresql123"):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.engine = None
    
    def connect(self):
        """Connect to PostgreSQL"""
        try:
            connection_string = f"postgresql://{self.connection_params['user']}:{self.connection_params['password']}@{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
            
            self.engine = create_engine(connection_string)
            
            # Simple connection test
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                test_result = result.fetchone()
                if test_result[0] == 1:
                    logger.info("✅ PostgreSQL connection successful!")
                    return True
                    
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            logger.info("Please check:")
            logger.info("1. PostgreSQL is running")
            logger.info("2. Username and password are correct")
            logger.info("3. Database exists")
            return False
    
    def load_csv_file(self, csv_path, table_name=None):
        """Load a single CSV file to PostgreSQL"""
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            logger.info(f"📖 Reading {csv_path.name}: {len(df)} rows, {len(df.columns)} columns")
            
            # Create table name from filename if not provided
            if not table_name:
                table_name = csv_path.stem.lower().replace('-', '_').replace(' ', '_')
            
            # Clean column names
            df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Handle date columns
            for col in df.columns:
                if 'date' in col.lower() and df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        pass  # Keep as string if conversion fails
            
            # Load to PostgreSQL
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            
            logger.info(f"✅ Successfully loaded {len(df)} rows into '{table_name}' table")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load {csv_path}: {e}")
            return False
    
    def load_all_csv_files(self, data_directory="data"):
        """Load all CSV files from the data directory"""
        data_path = Path(data_directory)
        
        if not data_path.exists():
            logger.error(f"❌ Data directory '{data_directory}' not found")
            return
        
        # Find all CSV files
        csv_files = list(data_path.rglob("*.csv"))
        
        if not csv_files:
            logger.warning(f"⚠️  No CSV files found in '{data_directory}'")
            return
        
        logger.info(f"🔍 Found {len(csv_files)} CSV files")
        
        successful_loads = 0
        for csv_file in csv_files:
            if self.load_csv_file(csv_file):
                successful_loads += 1
        
        logger.info(f"📊 Loaded {successful_loads}/{len(csv_files)} files successfully")
        return successful_loads
    
    def show_tables(self):
        """Show all tables and their row counts"""
        try:
            with self.engine.connect() as conn:
                # Get all tables
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """))
                
                tables = [row[0] for row in result]
                
                if not tables:
                    logger.info("No tables found in database")
                    return
                
                logger.info(f"📋 Database contains {len(tables)} tables:")
                
                for table in tables:
                    try:
                        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        row_count = count_result.fetchone()[0]
                        logger.info(f"  • {table}: {row_count:,} rows")
                    except Exception as e:
                        logger.warning(f"  • {table}: Error getting count - {e}")
        
        except Exception as e:
            logger.error(f"❌ Failed to show tables: {e}")
    
    def run_sample_query(self, table_name):
        """Run a sample query on a table"""
        try:
            query = f"SELECT * FROM {table_name} LIMIT 5"
            df = pd.read_sql(query, self.engine)
            
            print(f"\n📋 Sample data from '{table_name}' table:")
            print("=" * 60)
            print(df.to_string(index=False))
            print("=" * 60)
            
        except Exception as e:
            logger.warning(f"⚠️  Could not query {table_name}: {e}")
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("🔐 Database connection closed")

def main():
    """Main function - update password here"""
    
    print("=" * 60)
    print("BASIC CSV TO POSTGRESQL LOADER")
    print("=" * 60)
    
    # ⚠️ UPDATE YOUR PASSWORD HERE ⚠️
    loader = SimplePostgreSQLLoader(
        host="localhost",
        port="5432", 
        database="postgres",
        user="postgres",
        password="postgresql123"  # ← UPDATE THIS!
    )
    
    # Step 1: Connect to database
    if not loader.connect():
        print("\n❌ Could not connect to PostgreSQL")
        print("Please update the password in the script and try again")
        return
    
    # Step 2: Load CSV files
    print("\n🔄 Loading CSV files...")
    successful_loads = loader.load_all_csv_files()
    
    if successful_loads == 0:
        print("❌ No files were loaded successfully")
        return
    
    # Step 3: Show what was loaded
    print("\n📊 Database Summary:")
    loader.show_tables()
    
    # Step 4: Show sample data
    sample_tables = ['customers', 'products', 'transactions']
    for table in sample_tables:
        loader.run_sample_query(table)
    
    # Step 5: Close connection
    loader.close()
    
    print("\n" + "=" * 60)
    print("✅ CSV LOADING COMPLETED!")
    print("=" * 60)
    print("Your e-commerce data is now in PostgreSQL!")
    print("You can connect with: psql -U postgres -h localhost")

if __name__ == "__main__":
    main()