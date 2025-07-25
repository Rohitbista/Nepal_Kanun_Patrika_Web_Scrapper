import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import sys
from urllib.parse import urlencode
import sqlite3
import json
import re

class LegalCaseScraper:
    def __init__(self, output_db="legal_cases.db"): 
        self.mudda_type_arr = [
            "दुनियाबादी देवानी", 
            "सरकारबादी देवानी", 
            "दुनियावादी फौजदारी", 
            "सरकारवादी फौजदारी", 
            "रिट", 
            "निवेदन", 
            "विविध"
        ]
        self.successful_entries = 0
        self.not_entered_links = []
        self.still_not_entered_links = []
        self.output_db = output_db
        # Initialize SQLite database
        self.conn = sqlite3.connect(self.output_db)
        self.create_tables()

    def create_tables(self):
        """Create SQLite tables for scraped data and failed links"""
        cursor = self.conn.cursor()
        
        # Table for scraped case data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cases (
                लिङ्क TEXT UNIQUE,
                निर्णय_नं TEXT,
                भाग TEXT,
                मुद्दाको_किसिम TEXT,
                साल TEXT,
                महिना TEXT,
                अंक TEXT,
                फैसला_मिति TEXT,
                अदालत TEXT,
                न्यायाधीश TEXT,
                आदेश_मिति TEXT,
                केस_नम्बर TEXT,
                विषय TEXT,
                निवेदक TEXT,
                विपक्षी TEXT,
                प्रकरण TEXT,
                ठहर TEXT
            )
        ''')

        # Table for failed links
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS failed_links (
                मुद्दाको_किसिम TEXT,
                साल TEXT,
                लिङ्क TEXT
            )
        ''')

        self.conn.commit()

    def nepali_sal_to_english_sal(self, sal):
        if not sal:  # IMPROVEMENT 7: Handle None/empty input
            return ""
        """Convert Nepali numerals to English numerals"""
        nepali_to_english = {
            '०': '0', '१': '1', '२': '2', '३': '3', '४': '4',
            '५': '5', '६': '6', '७': '7', '८': '8', '९': '9'
        }
        try:
            return ''.join(nepali_to_english.get(char, char) for char in str(sal))
        except (TypeError, AttributeError):
            raise ValueError(f"Input must be a string containing Nepali numerals, got: {type(sal)}")
    
    def search_url(self, mudda_type, sal):
        """Generate search URL based on mudda_type and sal"""
        mudda_types = {name: str(idx + 1) for idx, name in enumerate(self.mudda_type_arr)}
        
        if mudda_type not in mudda_types:
            raise ValueError(f"Invalid mudda_type: {mudda_type}. Must be one of {self.mudda_type_arr}")
        
        english_sal = self.nepali_sal_to_english_sal(sal)
        base_url = "https://nkp.gov.np/"
        params = {
            "mudda_number": "",
            "faisala_date_from": "",
            "faisala_date_to": "",
            "mudda_type": mudda_types[mudda_type],
            "mudda_name": "",
            "badi": "",
            "pratibadi": "",
            "judge": "",
            "ijlas_type": "",
            "nirnaya_number": "",
            "faisala_type": "",
            "keywords": "",
            "edition": "",
            "year": english_sal,
            "month": "",
            "volume": "",
            "Submit": "खोज्‍नुहोस्"
        }
        return f"{base_url}?{urlencode(params)}#"
    
    def return_soup(self, url, max_retries=3):  # IMPROVEMENT 9: Add retry mechanism
        """Get soup object from URL with retry logic"""
        for attempt in range(max_retries):
            try:
                r = requests.get(url, timeout=30, headers={  # IMPROVEMENT 10: Longer timeout and headers
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    # Create folder if it doesn't exist
                    folder_name = "scraped_html"  # IMPROVEMENT 11: Better folder name
                    os.makedirs(folder_name, exist_ok=True)

                    # Sanitize the URL to create a safe filename
                    filename = re.sub(r'[^a-zA-Z0-9_-]', '_', url)[:100]  # limit length
                    filepath = os.path.join(folder_name, f"{filename}.html")
                    
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(r.text)
                    with open(filepath, "r", encoding="utf-8") as f:
                        html = f.read()
                    soup = BeautifulSoup(html, 'html.parser')
                    return soup
                else:
                    print(f"Attempt {attempt + 1}: Failed to retrieve {url}. Status code: {r.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    
            except requests.exceptions.RequestException as e:  # IMPROVEMENT 12: Better exception handling
                print(f"Attempt {attempt + 1}: Error scraping {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    
        return None

    def from_each_page(self, links):
        li = []
        flag = False
        i = 0
        while(i < len(links)):
            href = links[i].get('href')  # Get the value of the href attribute
            if href and "#" in href:  # IMPROVEMENT 13: Check href exists
                i+=1
                if i < len(links):  # IMPROVEMENT 14: Bounds checking
                    temp_href = links[i].get('href')
                    if temp_href:  # IMPROVEMENT 15: Check temp_href exists
                        li.append(temp_href)
            else:
                i+=1
        unique_list = []
        if(len(li) > 1):
            unique_list = list(dict.fromkeys(li))
        return unique_list
    
    def get_all_pages(self, initial_url):
        """Get all page URLs for pagination"""
        soup = self.return_soup(initial_url)
        if not soup:
            return []
            
        links = soup.find_all('a')
        all_links = []
        other_pages = []
        
        for link in links:
            href = link.get('href')
            if href:
                all_links.append(href)
                if "https://nkp.gov.np/advance_search/" in href:
                    other_pages.append(href)
        
        unique_list = self.from_each_page(links)
        
        # Handle pagination
        if "javascript:void(0)" in all_links and other_pages:
            mx = 0
            for j in other_pages:
                temp = ""
                for i in range(len(j)-1, -1, -1):
                    if j[i] == "=":
                        break
                    temp = j[i] + temp
                try:
                    temp2 = int(temp)
                    if mx < temp2:
                        mx = temp2
                except ValueError:
                    continue
            if mx > 0:
                st = other_pages[0][:-2]
                real_other_pages = []
                for i in range(20, mx + 1, 20):
                    real_other_pages.append(st + str(i))
                
                unique_list2 = []
                for page_url in real_other_pages:
                    print(f"Processing page: {page_url}")
                    try:
                        page_soup = self.return_soup(page_url)
                        if page_soup:
                            page_links = page_soup.find_all('a')
                            unique_list2 += self.from_each_page(page_links)
                    except Exception as e:
                        print(f"Error scraping page {page_url}: {e}")
                
                unique_list += unique_list2
        
        # Remove duplicates
        unique_unique_list = list(dict.fromkeys(unique_list))
        return unique_unique_list
    
    def get_edition_field(self, soup, label):
        """Extract edition field from soup"""
        edition_info = soup.find("div", id="edition-info")
        if edition_info:
            for span in edition_info.find_all("span"):
                if label in span.text:
                    strong = span.find("strong")
                    return strong.text.strip() if strong else None
        return None
    
    def scrape_case_details(self, url, mudda_type):
        """Scrape details from a single case URL"""
        try:
            r = requests.get(url, timeout=15)
            if r.status_code != 200:
                print(f"Failed to retrieve {url}, Status code: {r.status_code}")
                return False
            
            r.encoding = 'utf-8'
            # Create folder if it doesn't exist
            folder_name = "New Folder"
            os.makedirs(folder_name, exist_ok=True)

                # Sanitize the URL to create a safe filename
            filename = re.sub(r'[^a-zA-Z0-9_-]', '_', url)[:100]  # limit length
            filepath = os.path.join(folder_name, f"{filename}.html")
            
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(r.text)
            with open(filepath, "r", encoding="utf-8") as f:
                html = f.read()
            
            soup = BeautifulSoup(html, "html.parser")
            
            # Extract basic information
            title_tag = soup.find("h1", class_="post-title")
            decision_title = title_tag.get_text(strip=True).split()[2] if title_tag else "N/A"
            
            bhaag = self.get_edition_field(soup, "भाग")
            saal = self.get_edition_field(soup, "साल")
            mahina = self.get_edition_field(soup, "महिना")
            anka = self.get_edition_field(soup, "अंक")
            
            # Extract decision date
            post_meta = soup.find("div", class_="post-meta")
            decision_date = "N/A"
            if post_meta and "फैसला मिति" in post_meta.text:
                decision_date = post_meta.text.strip().split("फैसला मिति :")[-1].split("\n")[0].strip().split()[0]
            
            # Extract detailed information
            div_tag = soup.find("div", id="faisala_detail ")
            details = {}
            
            if div_tag:
                p_tags = div_tag.find_all('p')
                n = len(p_tags)
                ind = 0
                temp_ind_32 = ind
                KEYWORDS_2 = ["(प्रकरण नं", "(प्रकारण नं.", "९प्रकरण नं।", "(प्रकरण", "(प्र.नं."]
                # Extract court information
                while ind < n:
                    text = p_tags[ind].get_text(strip=True)
                    if text and "अदालत" in text:
                        details["अदालत"] = text
                        ind += 1
                        break
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                
                # Extract judges
                judges = []
                while ind < n:
                    text = p_tags[ind].get_text(strip=True)
                    if text:
                        if "न्यायाधीश" in text:
                            judges.append(text)
                        if "आदेश मिति" in text or "फैसला मिति" in text:
                            details["न्यायाधीश"] = judges
                            details["आदेश मिति"] = text
                            ind += 1
                            break
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                
                # Extract case details
                bisaya_before_kas_no = False
                while ind < n:
                    text = p_tags[ind].get_text(strip=True)
                    if text:
                        if "विषय" in text or "मुद्दा" in text:
                            bisaya_before_kas_no = True
                            details["विषय"] = text
                            ind += 1
                            break
                        details["केस_नम्बर"] = text
                        break
                    ind += 1

                if ind > n:
                    ind = temp_ind_32
                
                # Handle different case structures
                if bisaya_before_kas_no:
                    case_no = []
                    appellant = []
                    opposition = []
                    temp_flag = True
                    
                    while temp_flag and ind < n:
                        # Extract case number
                        while ind < n:
                            text = p_tags[ind].get_text(strip=True)
                            if text:
                                case_no.append(text)
                                ind += 1
                                break
                            ind += 1
                        
                        # Extract appellant
                        while ind < n:
                            text = p_tags[ind].get_text(strip=True)
                            if "निवेदक" in text or "प्रतिवादी" in text or "पुनरावेदक" in text:
                                appellant.append(text)
                                ind += 1
                                break
                            ind += 1
                        
                        # Extract opposition
                        while ind < n:
                            text = p_tags[ind].get_text(strip=True)
                            if "विपक्षी" in text or "वादी" in text or "प्रत्यर्थी" in text:
                                opposition.append(text)
                                ind += 1
                                break
                            ind += 1
                        
                        # Check for end condition
                        temp_ind = ind
                        for tag in p_tags[temp_ind:]:
                            text = tag.get_text(strip=True)
                            if "(प्रकरण नं" in text or "(प्रकारण नं." in text or "९प्रकरण नं।" in text or "(प्रकरण" in text:
                                temp_flag = False
                                details["केस_नम्बर"] = case_no
                                details["निवेदक"] = appellant
                                details["विपक्षी"] = opposition
                                break
                            elif "विरूद्ध"== text:
                                break

                    if ind >= n:
                        ind = temp_ind_32
                else:
                    # Standard case structure
                    while ind < n:
                        text = p_tags[ind].get_text(strip=True)
                        if "विषय" in text or "मुद्दा" in text:
                            details["विषय"] = text
                            ind += 1
                            break
                        ind += 1

                    if ind >= n:
                        ind = temp_ind_32
                    
                    while ind < n:
                        text = p_tags[ind].get_text(strip=True)
                        if "निवेदक" in text or "प्रतिवादी" in text or "पुनरावेदक" in text:
                            details["निवेदक"] = text
                            ind += 1
                            break
                        ind += 1

                    if ind >= n:
                        ind = temp_ind_32
                    
                    while ind < n:
                        text = p_tags[ind].get_text(strip=True)
                        if "विपक्षी" in text or "वादी" in text or "प्रत्यर्थी" in text:
                            details["विपक्षी"] = text
                            ind += 1
                            break
                        ind += 1

                    if ind >= n:
                        ind = temp_ind_32
                
                # Clean up extracted text
                # self.clean_extracted_details(details, bisaya_before_kas_no)
                
                # Extract prakarans and tahar
                prakarans = []
                prev = ""
                tahar = []
                temp_flag_tahar = False

                for tag in p_tags[ind:]:
                    text = tag.get_text(strip=True)
                    if text:
                        if any(kw in text for kw in KEYWORDS_2):
                            prakarans.append(prev)
                            prakarans.append(text)
                            prev = ""
                        else:
                            prev = prev + " " + text if prev else text
                        
                        if "फैसला"==text or "आदेश"==text or temp_flag_tahar:
                            temp_flag_tahar = True
                            tahar.append(text)


                    # --- Inserted check for <li> inside following siblings ---
                    next_sib = tag.find_next_sibling()
                    while next_sib and next_sib.name in ['ul', 'ol']:
                        # Loop through all <li> inside this list
                        for li in next_sib.find_all('li'):
                            li_text = li.get_text(strip=True)
                            if li_text:
                                #print(li_text)
                                if any(kw in li_text for kw in KEYWORDS_2):
                                    prakarans.append(prev)
                                    prakarans.append(li_text)
                                    prev = ""
                                else:
                                    if prev:
                                        prev = prev + " " + li_text
                                    else:
                                        prev = prev + li_text
                                if "फैसला" == li_text or "आदेश" == li_text or temp_flag_tahar:
                                    temp_flag_tahar = True
                                    tahar.append(li_text)
                        # Move to next sibling in case multiple lists follow
                        next_sib = next_sib.find_next_sibling()
                
                details["प्रकरण"] = prakarans
                details["ठहर"] = tahar
            
            # Combine all data, handling lists and strings appropriately
            data = {
                "लिङ्क": url,
                "निर्णय नं.": decision_title,
                "भाग": bhaag,
                "मुद्दाको किसिम": mudda_type,
                "साल": saal,
                "महिना": mahina,
                "अंक": anka,
                "फैसला मिति": f"'{decision_date}'",
                "अदालत": details.get("अदालत", "N/A"),
                "न्यायाधीश": json.dumps(details.get("न्यायाधीश", []), ensure_ascii=False),
                "आदेश मिति": details.get("आदेश मिति", "N/A"),
                "केस_नम्बर": json.dumps(details.get("केस_नम्बर", []), ensure_ascii=False) if isinstance(details.get("केस_नम्बर"), list) else details.get("केस_नम्बर", "N/A"),
                "विषय": details.get("विषय", "N/A"),
                "निवेदक": json.dumps(details.get("निवेदक", []), ensure_ascii=False) if isinstance(details.get("निवेदक"), list) else details.get("निवेदक", "N/A"),
                "विपक्षी": json.dumps(details.get("विपक्षी", []), ensure_ascii=False) if isinstance(details.get("विपक्षी"), list) else details.get("विपक्षी", "N/A"),
                "प्रकरण": json.dumps(details.get("प्रकरण", []), ensure_ascii=False),
                "ठहर": json.dumps(details.get("ठहर", []), ensure_ascii=False)
            }
            
            # Save to SQLite
            self.save_to_sqlite(data)
            print(f"{url} - Successfully Scraped and Entered")
            return True
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return False
    
    def save_to_sqlite(self, data):
        """Save data to SQLite database"""
        cursor = self.conn.cursor()
        
        try:  # IMPROVEMENT 26: Better error handling for database operations
            cursor.execute('''
                INSERT OR REPLACE INTO cases (  -- IMPROVEMENT 27: Use INSERT OR REPLACE
                    लिङ्क, निर्णय_नं, भाग, मुद्दाको_किसिम, साल, महिना, अंक, फैसला_मिति,
                    अदालत, न्यायाधीश, आदेश_मिति, केस_नम्बर, विषय, निवेदक, विपक्षी, प्रकरण, ठहर
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data["लिङ्क"],
                data["निर्णय नं."],
                data["भाग"],
                data["मुद्दाको किसिम"],
                data["साल"],
                data["महिना"],
                data["अंक"],
                data["फैसला मिति"],
                data["अदालत"],
                data["न्यायाधीश"],
                data["आदेश मिति"],
                data["केस_नम्बर"],
                data["विषय"],
                data["निवेदक"],
                data["विपक्षी"],
                data["प्रकरण"],
                data["ठहर"]
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            raise
    
    def save_failed_links(self, failed_links, mudda_type, sal):
        """Save failed links to SQLite database"""
        if failed_links:
            cursor = self.conn.cursor()
            for link in failed_links:
                cursor.execute('''
                    INSERT INTO failed_links (मुद्दाको_किसिम, साल, लिङ्क)
                    VALUES (?, ?, ?)
                ''', (mudda_type, sal, link))
            self.conn.commit()
    
    def run_scraper(self, mudda_type, sal):
        """Main method to run the scraper"""
        print(f"Starting scraper for mudda_type: {mudda_type}, sal: {sal}")
        
        # Validate inputs
        if mudda_type not in self.mudda_type_arr:
            raise ValueError(f"Invalid mudda_type. Must be one of: {self.mudda_type_arr}")
        
        # Generate search URL
        try:
            search_url = self.search_url(mudda_type, sal)
            print(f"Search URL: {search_url}")
        except Exception as e:
            print(f"Error generating search URL: {e}")
            return
        
        # Get all case URLs
        print("Fetching all case URLs...")
        case_urls = self.get_all_pages(search_url)
        
        if not case_urls:
            print("No case URLs found!")
            return
        
        print(f"Found {len(case_urls)} case URLs to scrape")
        
        # Scrape each case
        successful_count = 0
        failed_links = []
        
        for i, url in enumerate(case_urls, 1):
            print(f"Processing {i}/{len(case_urls)}: {url}")
            
            success = self.scrape_case_details(url, mudda_type)
            if success:
                successful_count += 1
            else:
                failed_links.append(url)
            
            # Add delay between requests
            time.sleep(2)
        
        # Retry failed links once
        if failed_links:
            print(f"\nRetrying {len(failed_links)} failed links...")
            still_failed = []
            
            for i, url in enumerate(failed_links, 1):
                print(f"Retrying {i}/{len(failed_links)}: {url}")
                
                success = self.scrape_case_details(url, mudda_type)
                if success:
                    successful_count += 1
                else:
                    still_failed.append(url)
                
                time.sleep(1)
            
            # Save permanently failed links
            self.save_failed_links(still_failed, mudda_type, sal)
            
            print(f"\nFinal Results:")
            print(f"Total links found: {len(case_urls)}")
            print(f"Successfully scraped: {successful_count}")
            print(f"Failed to scrape: {len(still_failed)}")
            
            if still_failed:
                print(f"Failed links saved to SQLite database: failed_links table")
        else:
            print(f"\nResults:")
            print(f"Total links found: {len(case_urls)}")
            print(f"Successfully scraped: {successful_count}")
        
        print(f"Scraped data saved to SQLite database: {self.output_db}")
        
        # Clean up temporary files
        for temp_file in ["temp_html_file.html", "temp_file.html"]:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def close(self):  # IMPROVEMENT 35: Add explicit close method
        """Explicitly close the database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()

    def __del__(self):
        """Close SQLite connection when the object is destroyed"""
        self.conn.close()


def main():
    """Main function to run the application"""
    output_db = "legal_cases.db"  # Default value
    
    # Get input from user or command line arguments
    if len(sys.argv) >= 3:
        mudda_type = sys.argv[1]
        sal = sys.argv[2]
        if len(sys.argv) > 3:  # CHANGE 8: Check if database name is provided
            output_db = sys.argv[3]
    else:
        # Create a temporary scraper just to show options
        temp_scraper = LegalCaseScraper()
        print("Available mudda_type options:")
        for i, option in enumerate(temp_scraper.mudda_type_arr, 1):
            print(f"{i}. {option}")
        temp_scraper.close()  # IMPROVEMENT 36: Clean up temporary scraper
        
        mudda_type = input("\nEnter mudda_type: ").strip()
        sal = input("Enter sal (e.g., २०७३): ").strip()
        db_input = input("Enter output SQLite database filename (default: legal_cases.db): ").strip()
        
        if db_input:  # IMPROVEMENT 37: Only override if user provides input
            output_db = db_input
    
    # CHANGE 9: Create scraper with the specified database name
    scraper = LegalCaseScraper(output_db)
    
    try:
        scraper.run_scraper(mudda_type, sal)  # CHANGE 10: Remove output_db parameter
    except Exception as e:
        print(f"Error: {e}")
    finally:
        scraper.close()  # IMPROVEMENT 38: Ensure database connection is closed


if __name__ == "__main__":
    main()
