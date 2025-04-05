import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import subprocess
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

csv_file = "loveporno_data.csv"
MAX_RETRIES = 3
RETRY_DELAY = 5
COMMIT_INTERVAL = 1000  # Commit every 1000 records

def init_csv():
    """Initialize CSV file if it doesn't exist"""
    if not os.path.exists(csv_file):
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["page", "title", "size", "download_link", "magnet"])
        logging.info("Initialized new CSV file")
        configure_git_lfs()
    else:
        logging.info(f"CSV file '{csv_file}' already exists")

def configure_git_lfs():
    """Configure Git LFS tracking"""
    try:
        subprocess.run(["git", "lfs", "track", csv_file], check=True)
        logging.info(f"Configured Git LFS to track {csv_file}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error configuring Git LFS: {e.stderr}")
        raise

def git_commit(message):
    """Commit CSV file to Git repository"""
    try:
        subprocess.run(["git", "add", csv_file], check=True)
        result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        if result.returncode == 0:
            subprocess.run(["git", "push"], check=True)
            logging.info(f"Git commit successful: {message}")
        else:
            logging.warning(f"No changes to commit: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git error: {e.stderr}")

def torrent_to_magnet(torrent_url):
    """Convert torrent URL to magnet link"""
    try:
        response = requests.get(torrent_url, headers=headers, timeout=10)
        response.raise_for_status()
        torrent_content = response.content
        info_hash = hashlib.sha1(torrent_content).hexdigest()
        magnet = f"magnet:?xt=urn:btih:{info_hash}"
        return magnet
    except Exception as e:
        logging.error(f"Failed to convert {torrent_url} to magnet: {e}")
        return "N/A"

def crawl_page(page_number, retries=0):
    """Crawl a single page"""
    url = f"https://loveporno.net/page/{page_number}/"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        items = soup.find_all('li')
        results = []
        
        for index, item in enumerate(items):
            link_tag = item.find('a', class_='production-block__li-wrapper')
            if not link_tag:
                continue
                
            link = link_tag.get('href')
            if not link:
                continue
                
            post_id = link.split('/')[-1].split('-')[0]
            download_link = f"https://cdntraffic.top/Domahi/files/{post_id}.torrent"
            magnet = torrent_to_magnet(download_link)
            
            title_span = item.find('span', class_='text')
            title = title_span.text.strip() if title_span else "N/A"
            
            size_span = item.find('div', class_='duration')
            size = size_span.find('span').text.strip() if size_span else "N/A"
            
            results.append({
                "page": page_number,
                "title": title,
                "size": size,
                "download_link": download_link,
                "magnet": magnet,
                "index": index
            })
        
        logging.info(f"Page {page_number}: Found {len(results)} items")
        return results
        
    except requests.RequestException as e:
        if retries < MAX_RETRIES:
            logging.warning(f"Retry {retries + 1}/{MAX_RETRIES} for page {page_number}: {e}")
            time.sleep(RETRY_DELAY)
            return crawl_page(page_number, retries + 1)
        logging.error(f"Failed to crawl page {page_number} after {MAX_RETRIES} attempts: {e}")
        return []

def crawl_pages(start_page, end_page):
    """Main crawling logic"""
    init_csv()
    total_records = 0
    pbar = tqdm(range(start_page, end_page - 1, -1), desc="Crawling pages")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_page = {executor.submit(crawl_page, page): page for page in pbar}
        for future in as_completed(future_to_page):
            page_number = future_to_page[future]
            try:
                results = future.result()
                if results:
                    results.sort(key=lambda x: x["index"])  # Preserve page order
                    with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                        writer = csv.writer(file)
                        for data in results:
                            writer.writerow([data["page"], data["title"], data["size"], 
                                          data["download_link"], data["magnet"]])
                            total_records += 1
                    
                    if total_records >= COMMIT_INTERVAL:
                        git_commit(f"Update data for {total_records} records up to page {page_number}")
                        total_records = 0
                    
            except Exception as e:
                logging.error(f"Error processing page {page_number}: {e}")
            
            time.sleep(1)  # Rate limiting
    
    if total_records > 0:
        git_commit(f"Final update for remaining {total_records} records")

if __name__ == "__main__":
    # Git config moved to GitHub Actions
    start_page = int(os.getenv("START_PAGE", 11765))
    end_page = int(os.getenv("END_PAGE", 1))
    logging.info(f"Starting crawl from page {start_page} to {end_page}")
    crawl_pages(start_page, end_page)
    logging.info(f"Data saved to {csv_file}")
