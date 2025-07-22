# web_scraper.py
# Web scraping module for e-commerce competitor data

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
from typing import List, Dict, Any
import json
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceScraper:
    """Web scraper for e-commerce competitor data"""
    
    def __init__(self, headless: bool = True, delay_range: tuple = (1, 3)):
        self.headless = headless
        self.delay_range = delay_range
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def _random_delay(self):
        """Add random delay to avoid being blocked"""
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)
        
    def _setup_selenium_driver(self):
        """Setup Selenium WebDriver"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920x1080')
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            return driver
        except Exception as e:
            logger.error(f"Failed to setup Chrome driver: {e}")
            logger.info("Please install ChromeDriver: https://chromedriver.chromium.org/")
            return None
    
    def scrape_product_listings(self, base_url: str, categories: List[str], max_pages: int = 3) -> pd.DataFrame:
        """
        Scrape product listings from an e-commerce site
        Note: This is a template - adjust selectors based on target site
        """
        products = []
        
        for category in categories:
            logger.info(f"Scraping category: {category}")
            
            for page in range(1, max_pages + 1):
                try:
                    # Construct URL (adjust based on site structure)
                    url = f"{base_url}/category/{category}?page={page}"
                    
                    response = self.session.get(url)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Extract product information (adjust selectors)
                    product_elements = soup.find_all('div', class_='product-item')
                    
                    for element in product_elements:
                        try:
                            product_data = self._extract_product_data(element, category)
                            if product_data:
                                products.append(product_data)
                        except Exception as e:
                            logger.warning(f"Failed to extract product data: {e}")
                            continue
                    
                    self._random_delay()
                    
                except Exception as e:
                    logger.error(f"Failed to scrape {url}: {e}")
                    continue
        
        return pd.DataFrame(products)
    
    def _extract_product_data(self, element, category: str) -> Dict[str, Any]:
        """Extract product data from HTML element"""
        try:
            # Adjust these selectors based on the target website
            name = element.find('h3', class_='product-name')
            price = element.find('span', class_='price')
            rating = element.find('div', class_='rating')
            image_url = element.find('img')
            
            return {
                'name': name.text.strip() if name else 'Unknown',
                'price': self._clean_price(price.text if price else '0'),
                'rating': self._extract_rating(rating) if rating else 0,
                'image_url': image_url.get('src') if image_url else '',
                'category': category,
                'scraped_at': datetime.now().isoformat(),
                'source': 'competitor_site'
            }
        except Exception as e:
            logger.warning(f"Error extracting product data: {e}")
            return None
    
    def _clean_price(self, price_text: str) -> float:
        """Clean and convert price text to float"""
        try:
            # Remove currency symbols and convert to float
            cleaned = ''.join(char for char in price_text if char.isdigit() or char == '.')
            return float(cleaned) if cleaned else 0.0
        except:
            return 0.0
    
    def _extract_rating(self, rating_element) -> float:
        """Extract rating from rating element"""
        try:
            # Look for rating in various formats
            rating_text = rating_element.text.strip()
            if 'out of' in rating_text:
                return float(rating_text.split()[0])
            elif '/5' in rating_text:
                return float(rating_text.split('/')[0])
            else:
                # Try to find star elements
                stars = rating_element.find_all('span', class_='star-filled')
                return float(len(stars)) if stars else 0.0
        except:
            return 0.0
    
    def scrape_reviews_selenium(self, product_urls: List[str], max_reviews: int = 50) -> pd.DataFrame:
        """Scrape product reviews using Selenium"""
        driver = self._setup_selenium_driver()
        if not driver:
            return pd.DataFrame()
        
        reviews = []
        
        try:
            for url in product_urls:
                logger.info(f"Scraping reviews from: {url}")
                
                driver.get(url)
                self._random_delay()
                
                # Wait for reviews to load
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "review-item"))
                    )
                except:
                    logger.warning(f"No reviews found for {url}")
                    continue
                
                # Extract reviews
                review_elements = driver.find_elements(By.CLASS_NAME, "review-item")[:max_reviews]
                
                for element in review_elements:
                    try:
                        review_data = self._extract_review_data(element, url)
                        if review_data:
                            reviews.append(review_data)
                    except Exception as e:
                        logger.warning(f"Failed to extract review: {e}")
                        continue
                
                self._random_delay()
                
        finally:
            driver.quit()
        
        return pd.DataFrame(reviews)
    
    def _extract_review_data(self, element, product_url: str) -> Dict[str, Any]:
        """Extract review data from Selenium element"""
        try:
            # Adjust these selectors based on the target website
            reviewer = element.find_element(By.CLASS_NAME, "reviewer-name")
            rating = element.find_element(By.CLASS_NAME, "review-rating")
            text = element.find_element(By.CLASS_NAME, "review-text")
            date = element.find_element(By.CLASS_NAME, "review-date")
            
            return {
                'product_url': product_url,
                'reviewer_name': reviewer.text.strip() if reviewer else 'Anonymous',
                'rating': self._extract_rating_from_text(rating.text if rating else ''),
                'review_text': text.text.strip() if text else '',
                'review_date': date.text.strip() if date else '',
                'scraped_at': datetime.now().isoformat()
            }
        except Exception as e:
            logger.warning(f"Error extracting review data: {e}")
            return None
    
    def _extract_rating_from_text(self, rating_text: str) -> float:
        """Extract numeric rating from text"""
        try:
            import re
            matches = re.findall(r'\d+\.?\d*', rating_text)
            return float(matches[0]) if matches else 0.0
        except:
            return 0.0
    
    def scrape_social_mentions(self, keywords: List[str], platforms: List[str] = ['reddit']) -> pd.DataFrame:
        """Scrape social media mentions (Reddit example)"""
        mentions = []
        
        for keyword in keywords:
            for platform in platforms:
                if platform == 'reddit':
                    mentions.extend(self._scrape_reddit_mentions(keyword))
                # Add other platforms as needed
        
        return pd.DataFrame(mentions)
    
    def _scrape_reddit_mentions(self, keyword: str) -> List[Dict[str, Any]]:
        """Scrape Reddit mentions for a keyword"""
        mentions = []
        
        try:
            # Reddit search URL
            url = f"https://www.reddit.com/search.json?q={keyword}&sort=new&limit=25"
            
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            
            for post in data.get('data', {}).get('children', []):
                post_data = post.get('data', {})
                mentions.append({
                    'platform': 'reddit',
                    'title': post_data.get('title', ''),
                    'content': post_data.get('selftext', ''),
                    'score': post_data.get('score', 0),
                    'comments': post_data.get('num_comments', 0),
                    'created_utc': post_data.get('created_utc', 0),
                    'subreddit': post_data.get('subreddit', ''),
                    'author': post_data.get('author', ''),
                    'url': f"https://reddit.com{post_data.get('permalink', '')}",
                    'keyword': keyword,
                    'scraped_at': datetime.now().isoformat()
                })
            
            self._random_delay()
            
        except Exception as e:
            logger.error(f"Failed to scrape Reddit mentions for {keyword}: {e}")
        
        return mentions
    
    def save_scraped_data(self, data: pd.DataFrame, filename: str, output_dir: str = "data/raw/external"):
        """Save scraped data to file"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Save as both CSV and JSON
        csv_path = os.path.join(output_dir, f"{filename}.csv")
        json_path = os.path.join(output_dir, f"{filename}.json")
        
        data.to_csv(csv_path, index=False)
        data.to_json(json_path, orient='records', indent=2)
        
        logger.info(f"Scraped data saved to {csv_path} and {json_path}")

def main():
    """Example usage of the scraper"""
    scraper = EcommerceScraper()
    
    # Example: Scrape product data (adjust URL and categories for real sites)
    # products = scraper.scrape_product_listings(
    #     base_url="https://example-ecommerce.com",
    #     categories=['electronics', 'clothing', 'books'],
    #     max_pages=2
    # )
    # scraper.save_scraped_data(products, "competitor_products")
    
    # Example: Scrape social mentions
    social_mentions = scraper.scrape_social_mentions(
        keywords=['ecommerce', 'online shopping', 'retail trends'],
        platforms=['reddit']
    )
    scraper.save_scraped_data(social_mentions, "social_mentions")
    
    logger.info("Scraping completed!")

if __name__ == "__main__":
    main()