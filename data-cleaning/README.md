# CDW Retail AI Assistant - Data Cleaning Pipeline

This directory contains the data cleaning pipeline for the CDW Retail AI Assistant project. The pipeline processes the `Product_Dataset_wImages.csv` file to clean the data, download product images, and generate analysis reports.

## Overview

The data cleaning pipeline performs the following operations:

1. **Data Loading**: Loads the CSV dataset with product information
2. **Image Download**: Downloads all product images from the `DownloadUrl` column
3. **Image Renaming**: Renames images to unique filenames based on product identifiers
4. **URL Update**: Updates the `ImageUrl` column to point to local image paths
5. **Unique Product Identification**: Finds all unique products with unique images
6. **Analysis Report**: Generates comprehensive statistics and analysis

## Files

- `data_cleaning_pipeline.py` - Main Python script for the data cleaning pipeline
- `run_cleaning.sh` - Bash script to run the pipeline
- `requirements.txt` - Python package dependencies
- `Product_Dataset_wImages.csv` - Input dataset (6,001 rows)
- `README.md` - This documentation file

## Output Files

After running the pipeline, the following files will be generated:

- `cleaned_product_dataset.csv` - Cleaned dataset with updated image URLs
- `unique_products_with_images.csv` - Dataset containing only unique products with images
- `data_analysis_report.json` - Comprehensive analysis report with statistics
- `images/` - Directory containing downloaded product images
- `data_cleaning.log` - Detailed log file of the cleaning process

## Dataset Structure

The input dataset contains 34 columns including:

- `ItemCode` - Unique product identifier
- `ProductName` - Product name
- `ProductLongTitle` - Detailed product title
- `ManufacturerName` - Product manufacturer
- `ImageUrl` - CDW image URL (will be updated to local paths)
- `DownloadUrl` - Direct download URL for images
- `WebTaxonomyTopLevelName` - Product category
- And many more product attributes...

## Usage

### Quick Start

1. Make sure you're in the `data-cleaning` directory
2. Ensure `Product_Dataset_wImages.csv` is present
3. Run the pipeline:

```bash
./run_cleaning.sh
```

### Manual Execution

1. Install dependencies:
```bash
pip3 install -r requirements.txt
```

2. Run the Python script:
```bash
python3 data_cleaning_pipeline.py
```

## Pipeline Features

### Image Processing
- **Parallel Downloads**: Uses ThreadPoolExecutor for efficient parallel image downloading
- **Error Handling**: Robust error handling for failed downloads
- **Duplicate Prevention**: Skips already downloaded images
- **Format Validation**: Verifies downloaded content is actually an image

### Data Cleaning
- **Product Name Sanitization**: Cleans product names for valid filenames
- **Unique Filename Generation**: Creates unique filenames using ItemCode + ProductName + URL hash
- **URL Mapping**: Maintains mapping between original URLs and new filenames

### Analysis Features
- **Unique Product Identification**: Groups by ItemCode to find unique products
- **Category Analysis**: Analyzes product distribution by categories
- **Manufacturer Analysis**: Analyzes product distribution by manufacturers
- **Image Statistics**: Provides detailed download and success statistics

## Image Naming Convention

Downloaded images follow this naming pattern:
```
{ItemCode}_{CLEANED_PRODUCT_NAME}_{URL_HASH}.jpg
```

Example:
- Original: `https://cdn.cs.1worldsync.com/1a/21/1a219dc0-936b-4a19-b33f-5c84aae21918.jpg`
- New: `5519317_ERGOTRON_DEEP_KB_TRAY_F_WORKFIT_TX_1a219dc0.jpg`

## Configuration

The pipeline can be configured by modifying the `CDWDataCleaner` class:

- `max_workers`: Number of parallel download threads (default: 10)
- `images_dir`: Directory for storing images (default: "images")
- Timeout settings for HTTP requests
- User-Agent headers for web requests

## Error Handling

The pipeline includes comprehensive error handling:

- **Network Errors**: Handles connection timeouts and HTTP errors
- **File System Errors**: Handles disk space and permission issues
- **Data Validation**: Validates image content types
- **Logging**: Detailed logging of all operations and errors

## Performance

- **Parallel Processing**: Downloads up to 10 images simultaneously
- **Progress Tracking**: Shows progress every 100 processed images
- **Memory Efficient**: Processes data in chunks to avoid memory issues
- **Resume Capability**: Skips already downloaded images

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure write permissions for the images directory
2. **Network Timeouts**: Increase timeout values for slow connections
3. **Disk Space**: Ensure sufficient disk space for image downloads
4. **Python Dependencies**: Install all required packages from requirements.txt

### Log Files

Check `data_cleaning.log` for detailed information about:
- Download progress and statistics
- Error messages and failed downloads
- Processing timestamps
- Performance metrics

## Sample Output

After successful execution, you'll see output like:

```
CDW DATA CLEANING PIPELINE - SUMMARY
============================================================
Total rows processed: 6,000
Unique products found: 1,234
Images downloaded: 1,180
Products with images: 1,150
Download success rate: 95.8%
============================================================
```

## Next Steps

After running the data cleaning pipeline:

1. Review the `data_analysis_report.json` for insights
2. Use `unique_products_with_images.csv` for product recommendations
3. Integrate the cleaned dataset into your AI assistant
4. Use the local image paths for faster image loading

## Support

For issues or questions:
1. Check the log file for error details
2. Verify all dependencies are installed
3. Ensure sufficient disk space and network connectivity
4. Review the configuration settings
