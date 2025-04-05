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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Headers from the successful request
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "DNT": "1",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="135", "Not-A.Brand";v="8"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1"
}

# Initial cookies from the successful request
initial_cookies = {
    "PHPSESSID": "c568b42325e574329d1e7786b9146807",
    "_ym_uid": "1743841157272382030",
    "_ym_d": "1743841157",
    "_ym_isad": "2"
}

csv_file = "loveporno_data.csv"
MAX_RETRIES = 20
RETRY_DELAY = 5
COMMIT_INTERVAL = 1000
TIMEOUT = 20
MAX_WORKERS = 5

# Configure requests session with retries and initial cookies
session = requests.Session()
session.cookies.update(initial_cookies)
retries = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

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
        response = session.get(torrent_url, headers=headers, timeout=TIMEOUT)
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
        headers["Referer"] = "https://loveporno.net/"
        response = session.get(url, headers=headers, timeout=TIMEOUT)
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
            delay = RETRY_DELAY * (2 ** retries)
            logging.warning(f"Retry {retries + 1}/{MAX_RETRIES} for page {page_number} after {delay}s: {e}")
            time.sleep(delay)
            return crawl_page(page_number, retries + 1)
        logging.error(f"Failed to crawl page {page_number} after {MAX_RETRIES} attempts: {e}")
        return []

def crawl_pages(start_page, end_page):
    """Main crawling logic"""
    init_csv()
    total_records = 0
    pbar = tqdm(range(start_page, end_page - 1, -1), desc="Crawling pages")
    
    # Initial request to establish session cookies
    try:
        session.get("https://loveporno.net/", headers=headers, timeout=TIMEOUT)
        logging.info("Initialized session with homepage request")
    except requests.RequestException as e:
        logging.warning(f"Failed to initialize session: {e}")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_page = {executor.submit(crawl_page, page): page for page in pbar}
        for future in as_completed(future_to_page):
            page_number = future_to_page[future]
            try:
                results = future.result()
                if results:
                    results.sort(key=lambda x: x["index"])
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
            
            time.sleep(2)
    
    if total_records > 0:
        git_commit(f"Final update for remaining {total_records} records")

if __name__ == "__main__":
    start_page = int(os.getenv("START_PAGE", 11765))
    end_page = int(os.getenv("END_PAGE", 1))
    logging.info(f"Starting crawl from page {start_page} to {end_page}")
    crawl_pages(start_page, end_page)
    logging.info(f"Data saved to {csv_file}")
