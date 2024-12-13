import csv
import subprocess
import concurrent.futures
import json
import pandas as pd
import time
from urllib.parse import urlparse
import logging
from datetime import datetime
import os
from typing import List, Dict, Any, Optional
import backoff
from ratelimit import limits, sleep_and_retry

# Configuration
CONFIG = {
    'API_URL': 'https://api-lr.agent.ai/api/company/lite',
    'SERVER_URL': 'http://51.12.241.183:80',
    'AUTH': {
        'username': 'ricarda',
        'password': '4712YYu'
    },
    'HEADERS': {
        'accept': '*/*',
        'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://agent.ai',
        'referer': 'https://agent.ai/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    },
    'MAX_WORKERS': 50,  # Reduced from 1000 for stability
    'MAX_RETRIES': 3
}

# Set up logging with both file and console output
def setup_logging():
    os.makedirs('logs', exist_ok=True)
    log_file = f'logs/api_calls_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

class TokenManager:
    def __init__(self):
        self.token = None
        self.last_refresh = None
        self.refresh_interval = 3600  # 1 hour

    def get_token(self) -> str:
        if (not self.token or 
            not self.last_refresh or 
            time.time() - self.last_refresh > self.refresh_interval):
            self.refresh_token()
        return self.token

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def refresh_token(self) -> None:
        try:
            command = [
                'curl', '--location', f'{CONFIG["SERVER_URL"]}/login',
                '--header', 'Content-Type: application/x-www-form-urlencoded',
                '--data-urlencode', f'username={CONFIG["AUTH"]["username"]}',
                '--data-urlencode', f'password={CONFIG["AUTH"]["password"]}'
            ]
            
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            response = json.loads(result.stdout)
            self.token = response.get('access_token')
            self.last_refresh = time.time()
            logging.info("Token refreshed successfully")
        except Exception as e:
            logging.error(f"Error refreshing token: {str(e)}")
            raise

class Report:
    def __init__(self):
        self.stats = {
            "success_count": 0,
            "error_count": 0,
            "start_time": datetime.now(),
            "processed_domains": set(),
            "failed_domains": set(),
            "error_types": {}
        }
    
    def update(self, success: bool, domain: Optional[str] = None, error_type: Optional[str] = None) -> None:
        if success:
            self.stats["success_count"] += 1
            if domain:
                self.stats["processed_domains"].add(domain)
        else:
            self.stats["error_count"] += 1
            if domain:
                self.stats["failed_domains"].add(domain)
            if error_type:
                self.stats["error_types"][error_type] = self.stats["error_types"].get(error_type, 0) + 1

    def save_report(self) -> None:
        os.makedirs('reports', exist_ok=True)
        report_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f'reports/scraping_report_{report_time}.json'
        
        with open(report_file, 'w') as f:
            json.dump({
                "success_count": self.stats["success_count"],
                "error_count": self.stats["error_count"],
                "runtime": str(datetime.now() - self.stats["start_time"]),
                "failed_domains": list(self.stats["failed_domains"]),
                "error_types": self.stats["error_types"]
            }, f, indent=2)

def clean_domain(url: str) -> tuple[str, str]:
    """Extract and clean domain from URL."""
    try:
        if not url.startswith(('http://', 'https://')):
            full_url = 'https://' + url
        else:
            full_url = url
        
        parsed = urlparse(full_url)
        domain = parsed.netloc
        
        if domain.startswith('www.'):
            domain = domain[4:]
            
        return domain.lower().strip(), full_url
    except Exception as e:
        logging.error(f"Error cleaning domain {url}: {str(e)}")
        return url, url

@sleep_and_retry
@limits(calls=100, period=60)
@backoff.on_exception(
    backoff.expo,
    (subprocess.CalledProcessError, json.JSONDecodeError),
    max_tries=CONFIG['MAX_RETRIES']
)
def call_api(website: str, report: Report) -> Optional[Dict[str, Any]]:
    """Call API with improved error handling and response validation."""
    try:
        clean_website, full_url = clean_domain(website)
        data = {
            "domain": clean_website,
            "report_component": "harmonic_funding_and_web_traffic",
            "user_id": None
        }

        command = [
            'curl', CONFIG['API_URL'],
            '-X', 'POST',
            *sum((['-H', f'{k}: {v}'] for k, v in CONFIG['HEADERS'].items()), []),
            '--data-raw', json.dumps(data)
        ]
        
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        # Validate response
        if not result.stdout.strip():
            logging.error(f"Empty response for {website}")
            report.update(success=False, domain=clean_website, error_type="empty_response")
            return None

        # Check for HTML response
        if result.stdout.strip().startswith(('<html', '<!DOCTYPE html')):
            logging.error(f"Received HTML instead of JSON for {website}")
            report.update(success=False, domain=clean_website, error_type="html_response")
            return None
        
        try:
            response_data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON for {website}: {str(e)}")
            report.update(success=False, domain=clean_website, error_type="invalid_json")
            return None
        
        if not isinstance(response_data, dict):
            logging.error(f"Invalid response type for {website}: {type(response_data)}")
            report.update(success=False, domain=clean_website, error_type="invalid_response_type")
            return None
        
        # Add metadata
        response_data.update({
            'original_url': website,
            'clean_domain': clean_website,
            'full_url': full_url
        })
        
        report.update(success=True, domain=clean_website)
        return response_data

    except Exception as e:
        logging.error(f"Error calling API for {website}: {str(e)}")
        report.update(success=False, domain=clean_website, error_type=str(type(e).__name__))
        return None

def read_csv_with_encoding_fallback(file_path: str, chunk_size: int):
    """Read CSV with multiple encoding attempts and robust error handling."""
    # Try different encodings in order of likelihood
    encodings = [
        'utf-8-sig',  # UTF-8 with BOM
        'cp1252',     # Windows German encoding
        'latin1',     # Western Europe
        'iso-8859-1', # Western Europe alternative
        'utf-16',     # Unicode
        'utf-32'      # Unicode alternative
    ]
    
    for encoding in encodings:
        try:
            # First try to read a small sample to validate encoding
            sample_size = 1024
            with open(file_path, 'rb') as f:
                raw_sample = f.read(sample_size)
            
            # Try to decode the sample
            raw_sample.decode(encoding)
            
            # If sample decoding worked, try reading the full file
            logging.info(f"Attempting to read CSV with {encoding} encoding")
            return pd.read_csv(
                file_path,
                chunksize=chunk_size,
                encoding=encoding,
                on_bad_lines='warn',  # Don't fail on problematic lines
                low_memory=False      # More permissive parsing
            )
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            logging.warning(f"Failed to read with {encoding} encoding: {str(e)}")
            continue
    
    # If all else fails, try binary read and force decode
    logging.warning("All standard encodings failed, attempting force decode")
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
        text = content.decode('utf-8', errors='replace')
        
        # Write to temporary file with safe encoding
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as temp:
            temp.write(text)
            temp_path = temp.name
            
        # Read from temporary file
        result = pd.read_csv(
            temp_path,
            chunksize=chunk_size,
            encoding='utf-8',
            on_bad_lines='warn',
            low_memory=False
        )
        
        # Clean up temp file in background
        import os
        os.unlink(temp_path)
        
        return result
        
    except Exception as e:
        logging.error(f"All attempts to read CSV failed: {str(e)}")
        raise RuntimeError("Unable to read CSV file with any encoding")

def process_website(website: str, report: Report, token_manager: TokenManager) -> None:
    """Process a single website with comprehensive error handling."""
    try:
        if not website.strip():
            logging.warning("Empty website URL skipped")
            return

        result = call_api(website, report)
        if result:
            mapped_data = map_company_data(result)
            if mapped_data:
                send_to_server(mapped_data, token_manager)
            
    except Exception as e:
        logging.error(f"Error processing {website}: {str(e)}")
        report.update(success=False, domain=website, error_type=str(type(e).__name__))

def main() -> None:
    try:
        setup_logging()
        token_manager = TokenManager()
        report = Report()
        
        # Initial token fetch
        token_manager.get_token()
        
        start_time = time.time()
        chunk_size = 1000  # Reduced from 5000 for better manageability
        
        # Process input file
        csv_reader = read_csv_with_encoding_fallback('gelbeurls.csv', chunk_size)
        
        for chunk_index, chunk in enumerate(csv_reader):
            logging.info(f"Processing chunk {chunk_index + 1}")
            websites = chunk.iloc[:, 0].tolist()
            
            # Process in smaller batches
            batch_size = 100  # Reduced from 500
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG['MAX_WORKERS']) as executor:
                for i in range(0, len(websites), batch_size):
                    batch = websites[i:i + batch_size]
                    futures = [
                        executor.submit(process_website, website, report, token_manager)
                        for website in batch
                    ]
                    concurrent.futures.wait(futures)
                    
                    # Add delay between batches
                    time.sleep(2)
            
            # Save intermediate report
            if chunk_index % 5 == 0:
                report.save_report()
        
        # Save final report
        report.save_report()
        
        execution_time = time.time() - start_time
        logging.info(f"Processing completed in {execution_time:.2f} seconds")
        
    except Exception as e:
        logging.error(f"Critical error in main process: {str(e)}")
        raise

if __name__ == '__main__':
    main()