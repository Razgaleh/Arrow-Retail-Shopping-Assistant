#!/usr/bin/env python3
"""
CDW Retail AI Assistant - Complete Data Processing Pipeline
==========================================================

This single script performs the complete data processing pipeline:
1. Loads the original Product_Dataset_wImages.csv
2. Downloads all product images for unique ProductNames
3. Cleans and transforms the data
4. Generates realistic prices
5. Removes all special characters and quotation marks
6. Creates the final cdw_products.csv file

Author: AI Assistant
Date: 2024
"""

import pandas as pd
import requests
import os
import re
import hashlib
import random
import time
import logging
from urllib.parse import urlparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cdw_data_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CDWDataProcessor:
    """Complete CDW data processing pipeline"""
    
    def __init__(self, input_file: str = "Product_Dataset_wImages.csv", images_dir: str = "images"):
        self.input_file = input_file
        self.images_dir = Path(images_dir)
        self.images_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for organization
        self.downloaded_dir = self.images_dir / "downloaded"
        self.downloaded_dir.mkdir(exist_ok=True)
        
        self.df = None
        self.image_mapping = {}  # Maps original URLs to new filenames
        self.download_stats = {
            'total_images': 0,
            'downloaded': 0,
            'failed': 0,
            'skipped': 0
        }
        
    def load_data(self) -> pd.DataFrame:
        """Load the CSV dataset with encoding detection"""
        logger.info(f"Loading dataset from {self.input_file}")
        
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                self.df = pd.read_csv(self.input_file, encoding=encoding)
                logger.info(f"Successfully loaded dataset with {encoding} encoding")
                break
            except UnicodeDecodeError:
                logger.warning(f"Failed to load with {encoding} encoding, trying next...")
                continue
        else:
            raise ValueError("Could not load CSV file with any of the attempted encodings")
        
        logger.info(f"Loaded {len(self.df)} rows with {len(self.df.columns)} columns")
        return self.df
    
    def clean_text(self, text):
        """Clean text for better embedding quality - remove special characters and HTML"""
        if pd.isna(text):
            return ""
        
        # Convert to string and strip whitespace
        text = str(text).strip()
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove HTML entities
        text = re.sub(r'&[a-zA-Z0-9#]+;', '', text)
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\-.,!?()]', '', text)
        
        # Remove quotation marks specifically
        text = text.replace('"', '')
        text = text.replace("'", '')
        
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove extra punctuation at the end
        text = re.sub(r'[.,!?]+$', '', text)
        
        # Clean up common issues
        text = text.replace('  ', ' ')
        text = text.replace(' ,', ',')
        text = text.replace(' .', '.')
        
        return text.strip()
    
    def clean_item_name(self, text):
        """Clean item names specifically for better readability"""
        if pd.isna(text):
            return "Unknown Product"
        
        # Convert to string and strip whitespace
        text = str(text).strip()
        
        # Remove special characters but keep spaces and basic punctuation
        text = re.sub(r'[^\w\s\-/]', '', text)
        
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Clean up common abbreviations and make more readable
        abbreviations = {
            'KB': 'Keyboard', 'MOUSE': 'Mouse', 'MON': 'Monitor', 'USB': 'USB', 'HD': 'HD',
            'WRLS': 'Wireless', 'PROGRAMMABLE': 'Programmable', 'ULTRASLIM': 'Ultra Slim',
            'GAMING': 'Gaming', 'MBA': 'MacBook Air', 'MBP': 'MacBook Pro', 'M2': 'M2',
            'M3': 'M3', 'M3P': 'M3 Pro', '8C8C': '8-Core', '11C14C': '11-Core',
            '16': '16GB', '18': '18GB', '256': '256GB', '512': '512GB', 'STR': 'Silver',
            'SB': 'Space Black', 'GRY': 'Gray', 'BLK': 'Black', 'WHT': 'White',
            'TP': 'Touchpad', 'NEMA': 'NEMA', 'INDUSTRIAL': 'Industrial',
            'BACKLIT': 'Backlit', 'SEALED': 'Sealed', 'WEBCAM': 'Webcam',
            '1080P': '1080p', 'RJ45': 'RJ45', 'PLUG': 'Plug', 'COVER': 'Cover',
            '6.0MM': '6.0mm', '50PK': '50 Pack', 'CUT': 'Cut', 'STRIP': 'Strip',
            'TOOL': 'Tool', 'RND': 'Round', 'FLAT': 'Flat', 'MULTI': 'Multi',
            'CONDUCTOR': 'Conductor', 'CUTTER': 'Cutter', 'STRIPPER': 'Stripper',
            'ADJUSTABLE': 'Adjustable', 'HEIGHT': 'Height', 'KEYBOARD': 'Keyboard',
            'TRAY': 'Tray', 'ACCESS': 'Access', 'MOUNT': 'Mount', 'ARM': 'Arm',
            'UNDERDESK': 'Under Desk', 'DRAWER': 'Drawer', 'BASIC': 'Basic',
            'LCD': 'LCD', 'MINI': 'Mini', 'TERM': 'Terminal', 'FEL': 'Fellowes',
            'GEL': 'Gel', 'WRISTREST': 'Wrist Rest', 'MOUSEPAD': 'Mousepad',
            'MICROBAN': 'Microban', 'ERGOTRON': 'Ergotron', 'DEEP': 'Deep',
            'WORKFIT': 'WorkFit', 'CHIEF': 'Chief', 'KEN': 'Kensington',
            'IKEY': 'iKey', 'C2G': 'C2G', '1RND': '1 Round', 'BENQ': 'BenQ',
            'EX3410R': 'EX3410R', 'MOBIUZ': 'Mobiuz', '34IN': '34 inch',
            'ALURATEK': 'Aluratek', 'AWC02F': 'AWC02F', 'APPLE': 'Apple',
            'ADESSO': 'Adesso', 'CHERRY': 'Cherry', 'COMPACT': 'Compact',
            'GENOVATION': 'Genovation', 'MINITERM': 'MiniTerm', 'KEYPAD': 'Keypad',
            'DARK': 'Dark', 'GRAY': 'Gray', 'INPUT': 'Input', 'DEVICE': 'Device'
        }
        
        for abbrev, full in abbreviations.items():
            text = text.replace(abbrev, full)
        
        # Clean up any remaining issues
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def detect_keyboard_or_mouse(self, row) -> Optional[str]:
        """Detect if a product is a keyboard or mouse based on name, category, and description"""
        def safe_str(value, default=''):
            """Safely convert value to string, handling NaN"""
            if pd.isna(value):
                return default
            return str(value).lower()
        
        product_name = safe_str(row.get('ProductName', ''))
        category = safe_str(row.get('WebTaxonomyTopLevelName', ''))
        subcategory = safe_str(row.get('WebTaxonomySubLevel1Name', ''))
        logistics_category = safe_str(row.get('LogisticsTaxonomyClassName', ''))
        logistics_group = safe_str(row.get('LogisticsTaxonomyGroupName', ''))
        description = safe_str(row.get('ProductLongTitle', '')) + ' ' + safe_str(row.get('MarketingText', ''))
        
        # Keywords for keyboard detection
        keyboard_keywords = ['keyboard', 'kb ', 'keypad', 'mechanical keyboard', 'wireless keyboard', 
                            'wired keyboard', 'gaming keyboard', 'ergonomic keyboard']
        # Exclude mouse-related keywords in keyboard context
        keyboard_exclude = ['mouse', 'trackball', 'track pad']
        
        # Keywords for mouse detection
        mouse_keywords = ['mouse', 'trackball', 'trackball mouse', 'wireless mouse', 
                         'wired mouse', 'gaming mouse', 'optical mouse', 'laser mouse']
        # Exclude keyboard-related keywords in mouse context
        mouse_exclude = ['keyboard', 'keypad']
        
        # Check if product name contains keyboard keywords (excluding mouse context)
        is_keyboard = False
        is_mouse = False
        
        # Check for bundle products - these should remain in Computer Accessories
        # Only check product name for bundle keywords, not description (to avoid false positives)
        if 'bundle' in product_name or 'combo' in product_name or 'cmbo' in product_name:
            return None  # Bundle products remain in their original category
        # Check if product name contains both keyboard and mouse (actual bundle)
        if 'keyboard' in product_name and 'mouse' in product_name:
            return None  # Bundle products remain in their original category
        
        # Check for keyboard - prioritize product name over subcategory
        for keyword in keyboard_keywords:
            if keyword in product_name:
                # Make sure it's not a mouse-related product
                if not any(exclude in product_name for exclude in keyboard_exclude):
                    is_keyboard = True
                    break
            elif keyword in subcategory or keyword in logistics_category or keyword in description:
                # Only check subcategory/description if not already detected from product name
                if not is_mouse and not any(exclude in product_name for exclude in keyboard_exclude):
                    is_keyboard = True
                    break
        
        # Check for mouse - prioritize product name over subcategory
        for keyword in mouse_keywords:
            if keyword in product_name:
                # Make sure it's not a keyboard-related product
                if not any(exclude in product_name for exclude in mouse_exclude):
                    is_mouse = True
                    break
            elif keyword in subcategory or keyword in logistics_category or keyword in description:
                # Only check subcategory/description if not already detected from product name
                if not is_keyboard and not any(exclude in product_name for exclude in mouse_exclude):
                    is_mouse = True
                    break
        
        # Additional checks based on category fields
        if 'keyboard' in category and 'mouse' not in category:
            is_keyboard = True
        elif 'keyboard' in subcategory and 'mouse' not in subcategory:
            is_keyboard = True
        elif 'keyboard' in logistics_category and 'mouse' not in logistics_category:
            is_keyboard = True
        elif 'keyboard' in logistics_group and 'mouse' not in logistics_group:
            is_keyboard = True
        
        if 'mouse' in category and 'keyboard' not in category:
            is_mouse = True
        elif 'mouse' in subcategory and 'keyboard' not in subcategory:
            is_mouse = True
        elif 'mouse' in logistics_category and 'keyboard' not in logistics_category:
            is_mouse = True
        elif 'mouse' in logistics_group and 'keyboard' not in logistics_group:
            is_mouse = True
        
        # Special handling for products that mention both - prioritize product name over subcategory
        if is_keyboard and is_mouse:
            # If product name clearly indicates one type, prioritize that
            if 'mouse' in product_name and 'keyboard' not in product_name:
                return 'Mice'
            elif 'keyboard' in product_name and 'mouse' not in product_name:
                return 'Keyboards'
            # If both in product name, check which is more prominent
            keyboard_count = sum(1 for kw in keyboard_keywords if kw in product_name)
            mouse_count = sum(1 for kw in mouse_keywords if kw in product_name)
            if keyboard_count > mouse_count:
                return 'Keyboards'
            elif mouse_count > keyboard_count:
                return 'Mice'
            elif keyboard_count == mouse_count and keyboard_count > 0:
                # If equal counts in product name, check which appears first
                if 'keyboard' in product_name and 'mouse' in product_name:
                    if product_name.find('keyboard') < product_name.find('mouse'):
                        return 'Keyboards'
                    else:
                        return 'Mice'
            # If product name doesn't clearly indicate, check subcategory
            keyboard_count_sub = sum(1 for kw in keyboard_keywords if kw in subcategory or kw in logistics_category)
            mouse_count_sub = sum(1 for kw in mouse_keywords if kw in subcategory or kw in logistics_category)
            if mouse_count_sub > keyboard_count_sub:
                return 'Mice'
            elif keyboard_count_sub > mouse_count_sub:
                return 'Keyboards'
            # Otherwise, don't categorize (keep original category)
            return None
        
        # Return the detected type
        if is_keyboard and not is_mouse:
            return 'Keyboards'
        elif is_mouse and not is_keyboard:
            return 'Mice'
        else:
            return None
    
    def generate_realistic_price(self, category, brand, product_name):
        """Generate realistic prices based on category, brand, and product type"""
        
        # Set random seed for consistent results
        random.seed(hash(product_name) % 1000)
        
        # Price ranges by category
        category_ranges = {
            'Keyboards': (20, 200),
            'Mice': (15, 150),
            'Computer Accessories': (15, 150),
            'Computers': (800, 3000),
            'Monitors': (200, 800),
            'Office Equipment & Supplies': (10, 100),
            'Electronics': (50, 500),
            'Do Not Use': (20, 200)  # Default range
        }
        
        # Brand multipliers (premium brands cost more)
        brand_multipliers = {
            'Apple Notebook Systems': 1.5,
            'Apple': 1.4,
            'BenQ Desktop Display': 1.2,
            'Cherry Keyboards': 1.1,
            'Kensington': 1.1,
            'Ergotron': 1.3,
            'Chief by Legrand AV': 1.2,
            'Cables To Go by Legrand': 0.9,
            'Fellowes': 1.0,
            'Genovation': 1.1,
            'IKEY': 1.0,
            'Adesso': 0.8,
            'Aluratek': 0.7,
            'Texas Instruments': 1.1,
            'Motorola Radios': 1.0,
            'Verbatim': 0.9,
            'Pi Engineering': 1.0
        }
        
        # Get base price range for category
        base_min, base_max = category_ranges.get(category, (20, 200))
        
        # Apply brand multiplier
        multiplier = brand_multipliers.get(brand, 1.0)
        
        # Generate price with some variation
        base_price = random.uniform(base_min, base_max)
        price = base_price * multiplier
        
        # Add some randomness based on product name length/complexity
        name_complexity = len(product_name.split()) / 10  # More words = slightly higher price
        price *= (1 + name_complexity * 0.1)
        
        # Round to realistic price points
        if price < 50:
            price = round(price, 2)  # Keep cents for lower prices
        elif price < 200:
            price = round(price, 1)  # Round to nearest dime
        else:
            price = round(price, 0)  # Round to nearest dollar
        
        # Ensure minimum price
        price = max(price, 5.99)
        
        return f"{price:.2f}"
    
    def generate_unique_filename(self, item_code: str, product_name: str, url: str) -> str:
        """Generate unique filename for image"""
        # Clean product name
        clean_name = self.clean_product_name(product_name)
        
        # Create base filename
        base_filename = f"{item_code}_{clean_name}"
        
        # Add URL hash to ensure uniqueness for different images of same product
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"{base_filename}_{url_hash}.jpg"
        
        return filename
    
    def clean_product_name(self, product_name: str) -> str:
        """Clean product name to create valid filename"""
        if pd.isna(product_name):
            return "UNKNOWN_PRODUCT"
        
        # Remove special characters and replace with underscores
        cleaned = re.sub(r'[^\w\s-]', '', str(product_name))
        # Replace spaces and multiple underscores with single underscore
        cleaned = re.sub(r'[\s_]+', '_', cleaned)
        # Remove leading/trailing underscores
        cleaned = cleaned.strip('_')
        # Limit length to avoid filesystem issues
        cleaned = cleaned[:100]
        
        return cleaned.upper() if cleaned else "UNKNOWN_PRODUCT"
    
    def download_image(self, url: str, filename: str) -> bool:
        """Download a single image"""
        try:
            if not url or pd.isna(url):
                return False
                
            # Check if file already exists
            filepath = self.downloaded_dir / filename
            if filepath.exists():
                logger.debug(f"Image already exists: {filename}")
                return True
            
            # Download image
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Verify it's an image
            content_type = response.headers.get('content-type', '').lower()
            if not content_type.startswith('image/'):
                logger.warning(f"URL does not point to image: {url}")
                return False
            
            # Save image
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            logger.debug(f"Downloaded: {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download {url}: {str(e)}")
            return False
    
    def download_images_batch(self, max_workers: int = 10) -> Dict[str, str]:
        """Download all images in parallel batches - only for products with unique ProductNames"""
        logger.info("Starting image download process...")
        
        # Get unique products by ProductName first
        unique_products_by_name = self.df.groupby('ProductName').first().reset_index()
        logger.info(f"Found {len(unique_products_by_name)} unique products by ProductName")
        
        # Get unique image URLs only for products with unique ProductNames
        unique_images = unique_products_by_name[['ItemCode', 'ProductName', 'DownloadUrl']].drop_duplicates()
        unique_images = unique_images.dropna(subset=['DownloadUrl'])
        
        logger.info(f"Found {len(unique_images)} unique images to download (only for unique ProductNames)")
        self.download_stats['total_images'] = len(unique_images)
        
        # Prepare download tasks
        download_tasks = []
        for _, row in unique_images.iterrows():
            filename = self.generate_unique_filename(
                str(row['ItemCode']), 
                str(row['ProductName']), 
                str(row['DownloadUrl'])
            )
            download_tasks.append((row['DownloadUrl'], filename))
        
        # Download images in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(self.download_image, url, filename): (url, filename)
                for url, filename in download_tasks
            }
            
            for future in as_completed(future_to_task):
                url, filename = future_to_task[future]
                try:
                    success = future.result()
                    if success:
                        self.download_stats['downloaded'] += 1
                        self.image_mapping[url] = filename
                    else:
                        self.download_stats['failed'] += 1
                except Exception as e:
                    logger.error(f"Task failed for {url}: {str(e)}")
                    self.download_stats['failed'] += 1
                
                # Progress update
                processed = self.download_stats['downloaded'] + self.download_stats['failed']
                if processed % 100 == 0:
                    logger.info(f"Progress: {processed}/{self.download_stats['total_images']} images processed")
        
        logger.info("Image download completed!")
        logger.info(f"Download statistics: {self.download_stats}")
        
        return self.image_mapping
    
    def update_image_urls(self) -> pd.DataFrame:
        """Update ImageUrl column to point to local image paths"""
        logger.info("Updating ImageUrl column to local paths...")
        
        def get_local_path(download_url):
            if pd.isna(download_url) or download_url not in self.image_mapping:
                return None
            return f"/images/{self.image_mapping[download_url]}"
        
        # Update the ImageUrl column
        self.df['ImageUrl'] = self.df['DownloadUrl'].apply(get_local_path)
        
        # Count updates
        updated_count = self.df['ImageUrl'].notna().sum()
        logger.info(f"Updated {updated_count} image URLs to local paths")
        
        return self.df
    
    def process_to_final_format(self) -> pd.DataFrame:
        """Process data to final embedding format"""
        logger.info("Processing data to final format...")
        
        # Get unique products by ProductName
        unique_products = self.df.groupby('ProductName').first().reset_index()
        logger.info(f"Found {len(unique_products)} unique products by ProductName")
        
        # Transform to embedding format
        embedding_data = []
        
        for _, row in unique_products.iterrows():
            # Skip products without images
            if pd.isna(row['ImageUrl']) or row['ImageUrl'] == '':
                continue
                
            # Create item description from available fields
            description_parts = []
            
            if not pd.isna(row['ProductLongTitle']):
                description_parts.append(str(row['ProductLongTitle']))
            
            if not pd.isna(row['MarketingText']):
                description_parts.append(str(row['MarketingText']))
                
            if not pd.isna(row['ObjectiveText']):
                description_parts.append(str(row['ObjectiveText']))
                
            if not pd.isna(row['KeySellingPoints']):
                # Clean up the key selling points (remove ~ separators)
                key_points = str(row['KeySellingPoints']).replace('~', ', ')
                description_parts.append(f"Key features: {key_points}")
            
            # Combine description parts
            item_description = '. '.join(description_parts)
            
            # Check if product is a keyboard or mouse for subcategory purposes
            keyboard_or_mouse = self.detect_keyboard_or_mouse(row)
            
            # Get category - use WebTaxonomyTopLevelName as primary category
            category = self.clean_text(row.get('WebTaxonomyTopLevelName', ''))
            if not category:
                category = self.clean_text(row.get('LogisticsTaxonomyTypeName', ''))
            
            # Set keyboards and mice to Computer Accessories category
            if keyboard_or_mouse:
                category = "Computer Accessories"
            elif not category:
                category = "Computer Accessories"  # Default category
            
            # Get brand/manufacturer
            brand = self.clean_text(row.get('ManufacturerName', ''))
            
            # Generate realistic price
            price = self.generate_realistic_price(category, brand, self.clean_text(row['ProductName']))
            
            # Get subcategory from source data
            source_subcategory = self.clean_text(row.get('WebTaxonomySubLevel1Name', ''))
            if not source_subcategory:
                source_subcategory = self.clean_text(row.get('LogisticsTaxonomyClassName', ''))
            
            # Override subcategory for keyboards and mice - prioritize detection over source data
            if keyboard_or_mouse == 'Keyboards':
                subcategory = "Keyboards"
            elif keyboard_or_mouse == 'Mice':
                subcategory = "Mice"
            elif source_subcategory:
                subcategory = source_subcategory
            else:
                subcategory = "General"  # Default subcategory
            
            # Get product URL
            product_url = str(row.get('ItemWebPage', '')) if not pd.isna(row.get('ItemWebPage', '')) else ''
            
            # Create the record with new column structure
            record = {
                'category': self.clean_text(category),
                'subcategory': self.clean_text(subcategory),
                'name': self.clean_item_name(row['ProductName']),
                'description': self.clean_text(item_description),
                'url': product_url,
                'price': price,
                'image': str(row['ImageUrl']) if not pd.isna(row['ImageUrl']) else ''
            }
            
            embedding_data.append(record)
        
        # Create DataFrame
        embedding_df = pd.DataFrame(embedding_data)
        
        # Remove duplicates based on name (just in case)
        embedding_df = embedding_df.drop_duplicates(subset=['name'])
        
        logger.info(f"Created {len(embedding_df)} unique products for final output")
        
        return embedding_df
    
    def save_final_csv(self, df: pd.DataFrame, output_file: str = "cdw_products.csv"):
        """Save the final CSV file without quotes"""
        logger.info(f"Saving final CSV to {output_file}...")
        
        # Replace commas in descriptions to avoid CSV quoting issues
        df_clean = df.copy()
        df_clean['description'] = df_clean['description'].str.replace(',', ';')
        df_clean.to_csv(output_file, index=False, quoting=0)
        
        logger.info(f"Final CSV saved with {len(df_clean)} rows")
    
    def run_complete_pipeline(self, max_workers: int = 10):
        """Run the complete data processing pipeline"""
        logger.info("Starting CDW Complete Data Processing Pipeline...")
        
        try:
            # Step 1: Load data
            self.load_data()
            
            # Step 2: Download images
            self.download_images_batch(max_workers=max_workers)
            
            # Step 3: Update image URLs
            self.update_image_urls()
            
            # Step 4: Process to final format
            final_df = self.process_to_final_format()
            
            # Step 5: Save final CSV
            self.save_final_csv(final_df)
            
            logger.info("Complete data processing pipeline completed successfully!")
            
            # Print summary
            print("\n" + "="*60)
            print("CDW COMPLETE DATA PROCESSING PIPELINE - SUMMARY")
            print("="*60)
            print(f"Total rows processed: {len(self.df):,}")
            print(f"Unique products found: {len(final_df):,}")
            print(f"Images downloaded: {self.download_stats['downloaded']:,}")
            print(f"Download success rate: {(self.download_stats['downloaded'] / self.download_stats['total_images'] * 100):.1f}%" if self.download_stats['total_images'] > 0 else "0%")
            
            # Price statistics
            prices = pd.to_numeric(final_df['price'], errors='coerce')
            print(f"Price range: ${prices.min():.2f} - ${prices.max():.2f}")
            print(f"Average price: ${prices.mean():.2f}")
            print("="*60)
            
            return final_df
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

def main():
    """Main function to run the complete data processing pipeline"""
    input_file = "Product_Dataset_wImages.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found!")
        print("Please make sure the file is in the current directory.")
        return
    
    # Initialize processor
    processor = CDWDataProcessor(input_file)
    
    # Run complete pipeline
    try:
        final_df = processor.run_complete_pipeline(max_workers=10)
        print("\nPipeline completed successfully!")
        print("Output files created:")
        print("- cdw_products.csv (final output)")
        print("- images/ directory with downloaded images")
        print("- cdw_data_processing.log")
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        logger.error(f"Pipeline failed: {str(e)}")

if __name__ == "__main__":
    main()
