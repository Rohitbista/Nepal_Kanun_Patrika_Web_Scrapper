import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import sys
import argparse
from urllib.parse import urlencode
import sqlite3
import json
import re
from pathlib import Path
import glob
import nepali_datetime

class LegalCaseScraper:
    def __init__(self, output_db="legal_cases_2.db", html_folder="scraped_html"):
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
        self.html_folder = html_folder
        
        # Create HTML folder if it doesn't exist
        os.makedirs(self.html_folder, exist_ok=True)
        
        # Initialize SQLite database
        self.conn = sqlite3.connect(self.output_db)
        self.create_tables()

    def create_tables(self):
        """Create SQLite tables for scraped data and failed links"""
        cursor = self.conn.cursor()
        
        # Table for scraped case data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                लिङ्क TEXT UNIQUE,
                निर्णय_नं TEXT,
                भाग TEXT,
                मुद्दाको_किसिम TEXT,
                साल TEXT,
                महिना TEXT,
                अंक TEXT,
                फैसला_मिति TEXT,
                अदालत_वा_इजलास TEXT,
                न्यायाधीश TEXT,
                आदेश_मिति TEXT,
                केस_नम्बर TEXT,
                विषय TEXT,
                निवेदक TEXT,
                विपक्षी TEXT,
                प्रकरण TEXT,
                ठहर TEXT,
                html_file_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Table for failed links
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS failed_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                मुद्दाको_किसिम TEXT,
                साल TEXT,
                लिङ्क TEXT,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.conn.commit()

    def get_mudda_type_number(self, mudda_type):
        """Get mudda type number (1-7) from mudda type name"""
        try:
            return str(self.mudda_type_arr.index(mudda_type) + 1)
        except ValueError:
            raise ValueError(f"Invalid mudda_type: {mudda_type}. Must be one of {self.mudda_type_arr}")

    def extract_link_number(self, url):
        """Extract the number at the end of the URL"""
        match = re.search(r'/(\d+)/?$', url)
        return match.group(1) if match else "unknown"

    def generate_html_filename(self, url, mudda_type, sal):
        """Generate standardized HTML filename: mudda_number_year_link_number.html"""
        mudda_number = self.get_mudda_type_number(mudda_type)
        english_sal = self.nepali_sal_to_english_sal(sal)
        link_number = self.extract_link_number(url)
        return f"{mudda_number}_{english_sal}_{link_number}.html"

    def nepali_sal_to_english_sal(self, sal):
        """Convert Nepali numerals to English numerals"""
        if not sal:
            return ""
        
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
    
    def save_html_file(self, url, html_content, mudda_type, sal):
        """Save HTML content to file with standardized naming"""
        filename = self.generate_html_filename(url, mudda_type, sal)
        filepath = os.path.join(self.html_folder, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        return filepath

    def load_html_file(self, url, mudda_type, sal):
        """Load HTML content from existing file"""
        filename = self.generate_html_filename(url, mudda_type, sal)
        filepath = os.path.join(self.html_folder, filename)
        
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                return f.read()
        return None

    def return_soup(self, url, mudda_type=None, sal=None, use_saved=True, max_retries=3):
        """Get soup object from URL or saved HTML file"""
        # Try to load from saved file first if requested
        if use_saved and mudda_type and sal:
            html_content = self.load_html_file(url, mudda_type, sal)
            if html_content:
                print(f"Using saved HTML file for {url}")
                return BeautifulSoup(html_content, 'html.parser')
        
        # Download from web if not found in saved files or use_saved is False
        for attempt in range(max_retries):
            try:
                r = requests.get(url, timeout=30, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    
                    # Save HTML file if mudda_type and sal are provided
                    if mudda_type and sal:
                        filepath = self.save_html_file(url, r.text, mudda_type, sal)
                        print(f"Saved HTML to: {filepath}")
                    
                    return BeautifulSoup(r.text, 'html.parser')
                else:
                    print(f"Attempt {attempt + 1}: Failed to retrieve {url}. Status code: {r.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1}: Error scraping {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    
        return None

    def get_saved_html_files_by_criteria(self, mudda_type=None, sal=None):
        """Get list of saved HTML files matching criteria"""
        pattern = "*"
        
        if mudda_type and sal:
            mudda_number = self.get_mudda_type_number(mudda_type)
            english_sal = self.nepali_sal_to_english_sal(sal)
            pattern = f"{mudda_number}_{english_sal}_*.html"
        elif sal:
            english_sal = self.nepali_sal_to_english_sal(sal)
            pattern = f"*_{english_sal}_*.html"
        elif mudda_type:
            mudda_number = self.get_mudda_type_number(mudda_type)
            pattern = f"{mudda_number}_*_*.html"
        
        search_path = os.path.join(self.html_folder, pattern)
        return glob.glob(search_path)

    def extract_info_from_filename(self, filename):
        """Extract mudda_type, sal, and link_number from filename"""
        basename = os.path.basename(filename)
        match = re.match(r'(\d+)_(\d+)_(\d+)\.html', basename)
        
        if match:
            mudda_number, sal, link_number = match.groups()
            mudda_type = self.mudda_type_arr[int(mudda_number) - 1]
            return mudda_type, sal, link_number
        
        return None, None, None

    def from_each_page(self, links):
        """Extract unique case links from page links"""
        li = []
        flag = False
        i = 0
        while(i < len(links)):
            href = links[i].get('href')
            if href and "#" in href:
                i+=1
                if i < len(links):
                    temp_href = links[i].get('href')
                    if temp_href:
                        li.append(temp_href)
            else:
                i+=1
        unique_list = []
        if(len(li) > 1):
            unique_list = list(dict.fromkeys(li))
        return unique_list
    
    def get_all_pages(self, initial_url, mudda_type=None, sal=None, use_saved=True):
        """Get all page URLs for pagination"""
        soup = self.return_soup(initial_url, mudda_type, sal, use_saved)
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
                        page_soup = self.return_soup(page_url, mudda_type, sal, use_saved)
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

    def determine_scraper_method(self, sal):
        """Determine which scraper method to use based on year"""
        eng_sal = int(self.nepali_sal_to_english_sal(sal))

        today = nepali_datetime.date.today()
        latest_nepali_year = int(today.year)
        
        if 2015 <= eng_sal <= 2044:
            return self.scrape_case_details_2015_to_2044
        elif 2045 <= eng_sal <= 2050:
            return self.scrape_case_details_2045_to_2050
        elif 2051 <= eng_sal <= 2061:
            return self.scrape_case_details_2051_to_2061
        elif 2062 <= eng_sal <= 2072:
            return self.scrape_case_details_2062_to_2072
        elif 2073 <= eng_sal < latest_nepali_year:
            return self.scrape_case_details_2073_to_2080_and_beyond
        else:
            raise ValueError(f"No scraper method available for year {eng_sal} or those records not yet available in Nepal Kanun Patrika Website")

    def scrape_case_details_generic(self, url, mudda_type, sal, use_saved=True):
        """Generic method that routes to the appropriate scraper based on year"""
        try:
            scraper_method = self.determine_scraper_method(sal)
            return scraper_method(url, mudda_type, sal, use_saved)
        except ValueError as e:
            print(f"Error: {e}")
            return False

    # [Previous scraper methods with modifications for HTML file handling]
    def scrape_case_details_2015_to_2044(self, url, mudda_type, sal=None, use_saved=True):
        """Scrape details from a single case URL (2015-2044)"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT लिङ्क FROM cases WHERE लिङ्क = ?', (url,))
            if cursor.fetchone():
                print(f"URL {url} already exists in database, skipping...")
                return True
            
            # Get soup using saved HTML or web
            soup = self.return_soup(url, mudda_type, sal, use_saved)
            if not soup:
                print(f"Failed to get content for {url}")
                return False
            
            # Extract basic information
            title_tag = soup.find("h1", class_="post-title")
            decision_title = title_tag.get_text(strip=True).split()[2] if title_tag and len(title_tag.get_text(strip=True).split()) > 2 else "N/A"
            
            bhaag = self.get_edition_field(soup, "भाग")
            saal = self.get_edition_field(soup, "साल")
            mahina = self.get_edition_field(soup, "महिना")
            anka = self.get_edition_field(soup, "अंक")
            
            # Extract decision date
            post_meta = soup.find("div", class_="post-meta")
            decision_date = "N/A"
            if post_meta and "फैसला मिति" in post_meta.text:
                try:
                    decision_date = post_meta.text.strip().split("फैसला मिति :")[-1].split("\n")[0].strip().split()[0]
                except IndexError:
                    decision_date = "N/A"
            
            # Extract detailed information
            div_tag = soup.find("div", id="faisala_detail ")
            details = {}
            
            if div_tag:
                tags = div_tag.find_all(['h1', 'p'])
                n = len(tags)
                ind = 0
                temp_ind_32 = ind
                KEYWORDS_2 = ["(प्रकरण नं", "(प्रकारण नं.", "९प्रकरण नं।", "(प्रकरण", "(प्र नं.","( प्र. नं","(प्र.नं", "(प्र. नं", "( प्रकरण नं.", "( प्रकरणन", "( प्र.नं.", "( प्र . नं .", "( प ्र . नं .", "(प्ररकण नं.", "(प्रकराण नं."]
                KEYWORDS_3 = ["निवेदक", "वादी", "पुनरावेदक", "निबेदक", "पुनरावदेक", "निवेदिका", "निवेदीका", "निवदेक", "न ि वेदक ः", "नि वेदक ः", "पुनरावेदन", "पुनरवेदिका", "पुनरावेदिका", "पुनरावेदीका", "बादि", "पुनराबेदक", "प्रतिबादी", "पुनरावेक", "अपीलाट", "निवेदनक", "उजुरवाला", "अपिलबाट", "अपिलाट"]
                KEYWORDS_4 = ["विपक्षी", "प्रतिवादी", "प्रत्यर्थी", "बिपक्षी", "विपक्षी ः", "पिपक्षी", "विरुद्ध", "प्रत्यार्थी", "विरूद्ध", "बिरूद्ध", "विपक्ष", "रेस्पोण्डेण्ट", "रेस्पोन्डेन्ट"]
                KEYWORDS_5 = ["विषय", "मुद्दा", "बिषय", "मूद्दा", "मुद्द", "मद्दा", "विपक्ष", "मुद्धा"]
                KEYWORDS_6 = ["इजलास", "इजालास", "इजलाश", "बेञ्च"]
                KEYWORDS_7 = ["आदेश", "फैसला", "फैसलमा", "निर्णय", "फै सला"]
                KEYWORDS_8 = ["न्यायाधीश", "माननीय", "न्यायधीश", "न्यायाधीस", "न्ययाधीश", "न्यायाधिश", "न्यायाधी", "न्यानायधीश", "नयायाधीश", "न्यायाधधिश", "नयाधश"]
                
                # Extract court information
                temp_ijlash = ""
                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if text:
                        if any(kw == text for kw in KEYWORDS_6):
                            if "निर्णय नं." not in temp_ijlash:
                                details["इजलास"] = temp_ijlash
                            ind+=1
                            break
                        elif any(kw in text for kw in KEYWORDS_6):
                            details["इजलास"] = text
                            ind+=1
                            text_2 = tags[ind].get_text(separator=' ', strip=True)
                            if any(kw in text_2 for kw in KEYWORDS_8) == False:
                                details["इजलास"] = text +" "+ text_2
                                ind+=1
                            break
                        elif any(kw in text for kw in KEYWORDS_8):
                            if "निर्णय नं." not in temp_ijlash:
                                details["इजलास"] = temp_ijlash
                            break
                        temp_ijlash = text
                    ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Extract judges
                judges = []
                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if text:
                        if any(kw in text for kw in KEYWORDS_8):
                            judges.append(text)
                        else:
                            details["न्यायाधीश"] = judges
                            if any(kw2 in text for kw2 in KEYWORDS_3) == False and any(kw2 in text for kw2 in KEYWORDS_5) == False:
                                details["केस_नम्बर"] = text
                                ind+=1
                            break
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Standard case structure      
                bisaya_before_niweduck = False
                temp_ind_64 = ind

                while temp_ind_64 < n:
                    text = tags[temp_ind_64].get_text(separator=' ', strip=True)
                    if any(kw in text for kw in KEYWORDS_3) or any(kw in text for kw in KEYWORDS_4):
                        break
                    if any(kw in text for kw in KEYWORDS_5):
                        bisaya_before_niweduck = True
                        break
                    temp_ind_64+=1
                
                if bisaya_before_niweduck:    
                    while ind < n:
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if any(text.startswith(kw) for kw in KEYWORDS_5):
                            details["विषय"] = text
                            ind+=1
                            break
                        if any(kw in text for kw in KEYWORDS_3):
                            ind = temp_ind_32
                            break
                        ind+=1
                else:
                    while ind  < n:
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if any(text.startswith(kw) for kw in KEYWORDS_7) and ("मिति" in text or "मिती" in text):
                            details["आदेश मिति"] = text
                            ind+=1
                            break
                        if any(kw in text for kw in KEYWORDS_3):
                            ind = temp_ind_32
                            break
                        ind+=1
                
                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                        
                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if any(kw in text for kw in KEYWORDS_3):
                        if any(kw2 == text for kw2 in KEYWORDS_3):
                            ind += 1
                            text = tags[ind].get_text(separator=' ', strip=True)
                        details["निवेदक"] = text
                        ind+=1
                        break
                    ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind

                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if any(kw in text for kw in KEYWORDS_4):
                        if any(kw2 == text for kw2 in KEYWORDS_4):
                            ind += 1
                            text = tags[ind].get_text(separator=' ', strip=True)
                        details["विपक्षी"] = text
                        ind+=1
                        break
                    ind+=1
                    
                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind

                if bisaya_before_niweduck==False:    
                    while ind < n:
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if any(text.startswith(kw) for kw in KEYWORDS_5):
                            details["विषय"] = text
                            ind+=1
                            break
                        if any(kw in text for kw in KEYWORDS_2):
                            ind = temp_ind_32
                            break
                        ind+=1
                else:
                    while ind  < n:
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if any(text.startswith(kw) for kw in KEYWORDS_7) and ("मिति" in text or "मिती" in text):
                            details["आदेश मिति"] = text
                            ind+=1
                            break
                        if any(kw in text for kw in KEYWORDS_2):
                            ind = temp_ind_32
                            break
                        ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Extract prakarans and tahar
                prakarans = []
                prev = ""
                tahar = []
                temp_flag_tahar = False

                for tag in tags[ind:]:
                    text = tag.get_text(separator=' ', strip=True)
                    if text:
                        #if "§" in text or any(kw in text for kw in KEYWORDS_2):
                            #prakarans.append(text)
                        if "§" in text or any(text.startswith(kw) for kw in KEYWORDS_2) or "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                            if any(text.startswith(kw) for kw in KEYWORDS_2):
                                if prev:  # IMPROVEMENT 23: Only append if prev has content
                                    prakarans.append(prev)
                                prakarans.append(text)
                                prev = ""
                            if "§" in text:
                                prakarans.append(text)
                            if "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                                if not prakarans:
                                    prakarans.append(prev)
                        else:
                            prev = prev + " " + text if prev else text
                        
                        if text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                            temp_flag_tahar = True
                            tahar.append(text)

                    # Process list items
                    next_sib = tag.find_next_sibling()
                    while next_sib and next_sib.name in ['ul', 'ol']:
                        for li in next_sib.find_all('li'):
                            li_text = li.get_text(separator=' ', strip=True)
                            if li_text:
                                if any(li_text.startswith(kw) for kw in KEYWORDS_2):
                                    if prev:  # IMPROVEMENT 24: Only append if prev has content
                                        prakarans.append(prev)
                                    prakarans.append(li_text)
                                    prev = ""
                                else:
                                    prev = prev + " " + li_text if prev else li_text
                                if li_text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                                    temp_flag_tahar = True
                                    tahar.append(li_text)
                        next_sib = next_sib.find_next_sibling()
                        
                details["प्रकरण"] = prakarans
                details["ठहर"] = tahar
            
            # Get HTML file path
            html_file_path = ""
            if mudda_type and sal:
                filename = self.generate_html_filename(url, mudda_type, sal)
                html_file_path = os.path.join(self.html_folder, filename)
            
            # Combine all data
            data = {
                "लिङ्क": url,
                "निर्णय नं.": decision_title,
                "भाग": bhaag or "N/A",
                "मुद्दाको किसिम": mudda_type,
                "साल": saal or "N/A",
                "महिना": mahina or "N/A",
                "अंक": anka or "N/A",
                "फैसला मिति": f"'{decision_date}'",
                "अदालत / इजलास": details.get("इजलास", "N/A"),
                "न्यायाधीश": json.dumps(details.get("न्यायाधीश", []), ensure_ascii=False),
                "आदेश मिति": details.get("आदेश मिति", "N/A"),
                "केस_नम्बर": json.dumps(details.get("केस_नम्बर", []), ensure_ascii=False) if isinstance(details.get("केस_नम्बर"), list) else details.get("केस_नम्बर", "N/A"),
                "विषय": details.get("विषय", "N/A"),
                "निवेदक": json.dumps(details.get("निवेदक", []), ensure_ascii=False) if isinstance(details.get("निवेदक"), list) else details.get("निवेदक", "N/A"),
                "विपक्षी": json.dumps(details.get("विपक्षी", []), ensure_ascii=False) if isinstance(details.get("विपक्षी"), list) else details.get("विपक्षी", "N/A"),
                "प्रकरण": json.dumps(details.get("प्रकरण", []), ensure_ascii=False),
                "ठहर": json.dumps(details.get("ठहर", []), ensure_ascii=False),
                "html_file_path": html_file_path
            }
            
            # Save to SQLite
            self.save_to_sqlite(data)
            print(f"{url} - Successfully Scraped and Entered")
            return True
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return False

    def scrape_case_details_2045_to_2050(self, url, mudda_type, sal = None, use_saved=True):  # CHANGE 4: Remove output_db parameter
        """Scrape details from a single case URL"""
        try:
            # IMPROVEMENT 16: Check if URL already exists in database
            cursor = self.conn.cursor()
            cursor.execute('SELECT लिङ्क FROM cases WHERE लिङ्क = ?', (url,))
            if cursor.fetchone():
                print(f"URL {url} already exists in database, skipping...")
                return True

            # Get soup using saved HTML or web
            soup = self.return_soup(url, mudda_type, sal, use_saved)
            if not soup:
                print(f"Failed to get content for {url}")
                return False
            
            # Extract basic information
            title_tag = soup.find("h1", class_="post-title")
            decision_title = title_tag.get_text(strip=True).split()[2] if title_tag and len(title_tag.get_text(strip=True).split()) > 2 else "N/A"  # IMPROVEMENT 19: Bounds checking
            
            bhaag = self.get_edition_field(soup, "भाग")
            saal = self.get_edition_field(soup, "साल")
            mahina = self.get_edition_field(soup, "महिना")
            anka = self.get_edition_field(soup, "अंक")
            
            # Extract decision date
            post_meta = soup.find("div", class_="post-meta")
            decision_date = "N/A"
            if post_meta and "फैसला मिति" in post_meta.text:
                try:  # IMPROVEMENT 20: Better error handling for date extraction
                    decision_date = post_meta.text.strip().split("फैसला मिति :")[-1].split("\n")[0].strip().split()[0]
                except IndexError:
                    decision_date = "N/A"
            
            # Extract detailed information
            div_tag = soup.find("div", id="faisala_detail ")
            details = {}
            
            if div_tag:
                tags = div_tag.find_all(['h1', 'p'])
                n = len(tags)
                ind = 0
                temp_ind_32 = ind
                KEYWORDS_2 = ["प्रकरण नं.", "(प्रकरण नं", "(प्रकारण नं.", "९प्रकरण नं।", "(प्रकरण", "(प्र नं.","( प्र. नं","(प्र.नं", "(प्र. नं", "( प्रकरण नं.", "( प्रकरणन", "( प्र.नं.", "( प्र . नं .", "( प ्र . नं .", "(प्ररकण नं.", "(प्रकराण नं."]
                KEYWORDS_3 = ["निवेदक", "वादी", "पुनरावेदक", "निबेदक", "पुनरावदेक", "निवेदिका", "निवेदीका", "निवदेक", "न ि वेदक ः", "नि वेदक ः", "पुनरावेदन", "पुनरवेदिका", "पुनरावेदिका", "पुनरावेदीका", "बादि", "पुनराबेदक", "प्रतिबादी", "पुनरावेक", "अपीलाट", "निवेदनक", "उजुरवाला", "अपिलबाट", "अपिलाट"]
                KEYWORDS_4 = ["विपक्षी", "प्रतिवादी", "प्रत्यर्थी", "बिपक्षी", "विपक्षी ः", "पिपक्षी", "प्रत्यार्थी", "विपक्ष", "रेस्पोण्डेण्ट", "रेस्पोन्डेन्ट", "प्रत्यथी"]
                KEYWORDS_5 = ["विषय", "मुद्दा", "बिषय", "मूद्दा", "मुद्द", "मद्दा", "विपक्ष", "मुद्धा", "मुद् दा"]
                KEYWORDS_6 = ["अदालत", "इजलास", "इजालास", "इजलाश", "बेञ्च"]
                KEYWORDS_7 = ["आदेश", "फैसला", "फैसलमा", "निर्णय", "फै सला", "मुद्दा"]
                KEYWORDS_8 = ["न्यायाधीश", "माननीय", "न्यायधीश", "न्यायाधीस", "न्ययाधीश", "न्यायाधिश", "न्यायाधी", "न्यानायधीश", "नयायाधीश", "न्यायाधधिश", "नयाधश"]
                KEYWORDS_9 = [ "विरूद्ध", "बिरूद्ध", "विरुद्ध", "बिरुद्ध"]
                KEYWORDS_10 = ["AP", "FN", "RE", "RI", "LE", "RV", "NF", "CI", "CR", "RC", "SA", "MS", "ND", "RB", "CF", "DF", "RF", "WO", "WH", "WS", "WF", "WC", "CC", "EC"]
                
                # Extract court information
                temp_ijlash = ""
                while(ind < n):
                    #text = p_tags[ind].get_text(strip=True)
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if text:
                        if any(kw == text for kw in KEYWORDS_6):
                            details["इजलास"] = temp_ijlash
                            ind+=1
                            break
                        elif any(kw in text for kw in KEYWORDS_6):
                            details["इजलास"] = text
                            ind+=1
                            break
                        elif "न्यायाधीश" in text or "माननीय" in text:
                            details["इजलास"] = temp_ijlash
                            break
                        temp_ijlash = text
                    ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Extract judges
                judges = []
                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if text:
                        if "न्यायाधीश" in text or "माननीय" in text:
                            judges.append(text)
                        elif any(kw in text for kw in KEYWORDS_7) and "मिति" in text:
                            details["न्यायाधीश"] = judges
                            details["आदेश मिति"] = text
                            ind += 1
                            break
                        else:
                            details["केस_नम्बर"] = text
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Standard case structure    
                bisaya_before_niweduck = False
                details["विषय"] = ""

                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if any(kw in text for kw in KEYWORDS_3) or any(kw in text for kw in KEYWORDS_4):
                        break
                    if any(kw in text for kw in KEYWORDS_5):
                        bisaya_before_niweduck = True
                        break
                    ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind

                if bisaya_before_niweduck:    
                    while ind < n:
                        #text = p_tags[ind].get_text(strip=True)
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if any(kw in text for kw in KEYWORDS_5):
                            details["विषय"] = text
                            ind+=1
                            break
                        ind+=1
                
                    if ind >= n:
                        ind = temp_ind_32
                    else:
                        temp_ind_32 = ind
                        
                    #temp_Ind = ind
                while ind < n:
                        #text = p_tags[ind].get_text(strip=True)
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if any(kw in text for kw in KEYWORDS_3):
                        if any(kw2 == text for kw2 in KEYWORDS_3):
                            ind += 1
                            text = tags[ind].get_text(separator=' ', strip=True)
                        details["निवेदक"] = text
                        ind+=1
                        break
                    ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind

                while ind < n:
                        #text = p_tags[ind].get_text(strip=True)
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if any(kw in text for kw in KEYWORDS_4):
                        if any(kw2 == text for kw2 in KEYWORDS_4):
                            ind += 1
                            text = tags[ind].get_text(separator=' ', strip=True)
                        details["विपक्षी"] = text
                        ind+=1
                        break
                    ind+=1
                    
                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind

                if bisaya_before_niweduck==False:    
                    while ind < n:
                        #text = p_tags[ind].get_text(strip=True)
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if any(kw in text for kw in KEYWORDS_5):
                            details["विषय"] = text
                            ind+=1
                            break
                        ind+=1
                
                    if ind >= n:
                        ind = temp_ind_32
                    else:
                        temp_ind_32 = ind
                
                # Extract prakarans and tahar
                prakarans = []
                prev = ""


                tahar = []
                temp_flag_tahar = False

                for tag in tags[ind:]:
                    text = tag.get_text(separator=' ', strip=True)
                    if text:
                        #if "§" in text or any(kw in text for kw in KEYWORDS_2):
                            #prakarans.append(text)
                        if "§" in text or any(text.startswith(kw) for kw in KEYWORDS_2) or "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                            if any(text.startswith(kw) for kw in KEYWORDS_2):
                                if prev:  # IMPROVEMENT 23: Only append if prev has content
                                    prakarans.append(prev)
                                prakarans.append(text)
                                prev = ""
                            if "§" in text:
                                prakarans.append(text)
                            if "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                                if not prakarans:
                                    prakarans.append(prev)
                        else:
                            prev = prev + " " + text if prev else text
                        
                        if text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                            temp_flag_tahar = True
                            tahar.append(text)

                    # Process list items
                    next_sib = tag.find_next_sibling()
                    while next_sib and next_sib.name in ['ul', 'ol']:
                        for li in next_sib.find_all('li'):
                            li_text = li.get_text(separator=' ', strip=True)
                            if li_text:
                                if any(li_text.startswith(kw) for kw in KEYWORDS_2):
                                    if prev:  # IMPROVEMENT 24: Only append if prev has content
                                        prakarans.append(prev)
                                    prakarans.append(li_text)
                                    prev = ""
                                else:
                                    prev = prev + " " + li_text if prev else li_text
                                if li_text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                                    temp_flag_tahar = True
                                    tahar.append(li_text)
                        next_sib = next_sib.find_next_sibling()
                
                details["प्रकरण"] = prakarans
                details["ठहर"] = tahar

            # Get HTML file path
            html_file_path = ""
            if mudda_type and sal:
                filename = self.generate_html_filename(url, mudda_type, sal)
                html_file_path = os.path.join(self.html_folder, filename)
            
            # Combine all data, handling lists and strings appropriately
            data = {
                "लिङ्क": url,
                "निर्णय नं.": decision_title,
                "भाग": bhaag or "N/A",  # IMPROVEMENT 25: Handle None values
                "मुद्दाको किसिम": mudda_type,
                "साल": saal or "N/A",
                "महिना": mahina or "N/A",
                "अंक": anka or "N/A",
                "फैसला मिति": f"'{decision_date}'",
                "अदालत / इजलास": details.get("इजलास", "N/A"),
                "न्यायाधीश": json.dumps(details.get("न्यायाधीश", []), ensure_ascii=False),
                "आदेश मिति": details.get("आदेश मिति", "N/A"),
                "केस_नम्बर": json.dumps(details.get("केस_नम्बर", []), ensure_ascii=False) if isinstance(details.get("केस_नम्बर"), list) else details.get("केस_नम्बर", "N/A"),
                "विषय": details.get("विषय", "N/A"),
                "निवेदक": json.dumps(details.get("निवेदक", []), ensure_ascii=False) if isinstance(details.get("निवेदक"), list) else details.get("निवेदक", "N/A"),
                "विपक्षी": json.dumps(details.get("विपक्षी", []), ensure_ascii=False) if isinstance(details.get("विपक्षी"), list) else details.get("विपक्षी", "N/A"),
                "प्रकरण": json.dumps(details.get("प्रकरण", []), ensure_ascii=False),
                "ठहर": json.dumps(details.get("ठहर", []), ensure_ascii=False),
                "html_file_path": html_file_path
            }
            
            # Save to SQLite
            self.save_to_sqlite(data)
            print(f"{url} - Successfully Scraped and Entered")
            return True
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return False

    def scrape_case_details_2051_to_2061(self, url, mudda_type, sal = None, use_saved=True):  # CHANGE 4: Remove output_db parameter
        """Scrape details from a single case URL"""
        try:
            # IMPROVEMENT 16: Check if URL already exists in database
            cursor = self.conn.cursor()
            cursor.execute('SELECT लिङ्क FROM cases WHERE लिङ्क = ?', (url,))
            if cursor.fetchone():
                print(f"URL {url} already exists in database, skipping...")
                return True

            # Get soup using saved HTML or web
            soup = self.return_soup(url, mudda_type, sal, use_saved)
            if not soup:
                print(f"Failed to get content for {url}")
                return False
            
            # Extract basic information
            title_tag = soup.find("h1", class_="post-title")
            decision_title = title_tag.get_text(strip=True).split()[2] if title_tag and len(title_tag.get_text(strip=True).split()) > 2 else "N/A"  # IMPROVEMENT 19: Bounds checking
            
            bhaag = self.get_edition_field(soup, "भाग")
            saal = self.get_edition_field(soup, "साल")
            mahina = self.get_edition_field(soup, "महिना")
            anka = self.get_edition_field(soup, "अंक")
            
            # Extract decision date
            post_meta = soup.find("div", class_="post-meta")
            decision_date = "N/A"
            if post_meta and "फैसला मिति" in post_meta.text:
                try:  # IMPROVEMENT 20: Better error handling for date extraction
                    decision_date = post_meta.text.strip().split("फैसला मिति :")[-1].split("\n")[0].strip().split()[0]
                except IndexError:
                    decision_date = "N/A"
            
            # Extract detailed information
            div_tag = soup.find("div", id="faisala_detail ")
            details = {}
            
            if div_tag:
                tags = div_tag.find_all(['h1', 'p'])
                n = len(tags)
                ind = 0
                temp_ind_32 = ind
                KEYWORDS_2 = ["प्रकरण नं.", "(प्रकरण नं", "(प्रकारण नं.", "९प्रकरण नं।", "(प्रकरण", "(प्र नं.","( प्र. नं","(प्र.नं", "(प्र. नं", "( प्रकरण नं.", "( प्रकरणन", "( प्र.नं.", "( प्र . नं .", "( प ्र . नं .", "(प्ररकण नं.", "(प्रकराण नं."]
                KEYWORDS_3 = ["निवेदक", "वादी", "पुनरावेदक", "निबेदक", "पुनरावदेक", "निवेदिका", "निवेदीका", "निवदेक", "न ि वेदक ः", "नि वेदक ः", "पुनरावेदन", "पुनरवेदिका", "पुनरावेदिका", "पुनरावेदीका", "बादि", "पुनराबेदक", "प्रतिबादी", "पुनरावेक", "अपीलाट", "निवेदनक", "उजुरवाला", "अपिलबाट", "अपिलाट"]
                KEYWORDS_4 = ["विपक्षी", "प्रतिवादी", "प्रत्यर्थी", "बिपक्षी", "विपक्षी ः", "पिपक्षी", "प्रत्यार्थी", "विपक्ष", "रेस्पोण्डेण्ट", "रेस्पोन्डेन्ट", "प्रत्यथी"]
                KEYWORDS_5 = ["विषय", "मुद्दा", "बिषय", "मूद्दा", "मुद्द", "मद्दा", "विपक्ष", "मुद्धा", "मुद् दा"]
                KEYWORDS_6 = ["अदालत", "इजलास", "इजालास", "इजलाश", "बेञ्च"]
                KEYWORDS_7 = ["आदेश", "फैसला", "फैसलमा", "निर्णय", "फै सला", "मुद्दा"]
                KEYWORDS_8 = ["न्यायाधीश", "माननीय", "न्यायधीश", "न्यायाधीस", "न्ययाधीश", "न्यायाधिश", "न्यायाधी", "न्यानायधीश", "नयायाधीश", "न्यायाधधिश", "नयाधश"]
                KEYWORDS_9 = [ "विरूद्ध", "बिरूद्ध", "विरुद्ध", "बिरुद्ध"]
                KEYWORDS_10 = ["AP", "FN", "RE", "RI", "LE", "RV", "NF", "CI", "CR", "RC", "SA", "MS", "ND", "RB", "CF", "DF", "RF", "WO", "WH", "WS", "WF", "WC", "CC", "EC"]
                
                # Extract court information
                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if text and ("इजलास" in text or "इजालास" in text):
                        details["इजलास"] = text
                        ind += 1
                        break
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Extract judges
                judges = []
                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if text:
                        if "न्यायाधीश" in text or "माननीय" in text:
                            judges.append(text)
                        elif ("आदेश" in text or "फैसला" in text or "फैसलमा" in text or "निर्णय" in text) and "मिति" in text:
                            details["न्यायाधीश"] = judges
                            details["आदेश मिति"] = text
                            ind += 1
                            break
                        else:
                            details["केस_नम्बर"] = text
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Standard case structure
                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if "विषय" in text or "मुद्दा" in text or "बिषय" in text or "मूद्दाः" in text:
                        details["विषय"] = text
                        ind += 1
                        break
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                    
                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if any(kw in text for kw in KEYWORDS_3):
                        if any(kw2 == text for kw2 in KEYWORDS_3):
                            ind += 1
                            if ind < n:  # IMPROVEMENT 21: Bounds checking
                                text = tags[ind].get_text(separator=' ', strip=True)
                        details["निवेदक"] = text
                        ind += 1
                        break
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                    
                while ind < n:
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if any(kw in text for kw in KEYWORDS_4):
                        if any(kw2 == text for kw2 in KEYWORDS_4):
                            ind += 1
                            if ind < n:  # IMPROVEMENT 22: Bounds checking
                                text = tags[ind].get_text(separator=' ', strip=True)
                        details["विपक्षी"] = text
                        ind += 1
                        break
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Extract prakarans and tahar
                prakarans = []
                prev = ""


                tahar = []
                temp_flag_tahar = False

                for tag in tags[ind:]:
                    text = tag.get_text(separator=' ', strip=True)
                    if text:
                        #if "§" in text or any(kw in text for kw in KEYWORDS_2):
                            #prakarans.append(text)
                        if "§" in text or any(text.startswith(kw) for kw in KEYWORDS_2) or "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                            if any(text.startswith(kw) for kw in KEYWORDS_2):
                                if prev:  # IMPROVEMENT 23: Only append if prev has content
                                    prakarans.append(prev)
                                prakarans.append(text)
                                prev = ""
                            if "§" in text:
                                prakarans.append(text)
                            if "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                                if not prakarans:
                                    prakarans.append(prev)
                        else:
                            prev = prev + " " + text if prev else text
                        
                        if text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                            temp_flag_tahar = True
                            tahar.append(text)

                    # Process list items
                    next_sib = tag.find_next_sibling()
                    while next_sib and next_sib.name in ['ul', 'ol']:
                        for li in next_sib.find_all('li'):
                            li_text = li.get_text(separator=' ', strip=True)
                            if li_text:
                                if any(li_text.startswith(kw) for kw in KEYWORDS_2):
                                    if prev:  # IMPROVEMENT 24: Only append if prev has content
                                        prakarans.append(prev)
                                    prakarans.append(li_text)
                                    prev = ""
                                else:
                                    prev = prev + " " + li_text if prev else li_text
                                if li_text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                                    temp_flag_tahar = True
                                    tahar.append(li_text)
                        next_sib = next_sib.find_next_sibling()
                
                details["प्रकरण"] = prakarans
                details["ठहर"] = tahar

            # Get HTML file path
            html_file_path = ""
            if mudda_type and sal:
                filename = self.generate_html_filename(url, mudda_type, sal)
                html_file_path = os.path.join(self.html_folder, filename)
            
            # Combine all data, handling lists and strings appropriately
            data = {
                "लिङ्क": url,
                "निर्णय नं.": decision_title,
                "भाग": bhaag or "N/A",  # IMPROVEMENT 25: Handle None values
                "मुद्दाको किसिम": mudda_type,
                "साल": saal or "N/A",
                "महिना": mahina or "N/A",
                "अंक": anka or "N/A",
                "फैसला मिति": f"'{decision_date}'",
                "अदालत / इजलास": details.get("इजलास", "N/A"),
                "न्यायाधीश": json.dumps(details.get("न्यायाधीश", []), ensure_ascii=False),
                "आदेश मिति": details.get("आदेश मिति", "N/A"),
                "केस_नम्बर": json.dumps(details.get("केस_नम्बर", []), ensure_ascii=False) if isinstance(details.get("केस_नम्बर"), list) else details.get("केस_नम्बर", "N/A"),
                "विषय": details.get("विषय", "N/A"),
                "निवेदक": json.dumps(details.get("निवेदक", []), ensure_ascii=False) if isinstance(details.get("निवेदक"), list) else details.get("निवेदक", "N/A"),
                "विपक्षी": json.dumps(details.get("विपक्षी", []), ensure_ascii=False) if isinstance(details.get("विपक्षी"), list) else details.get("विपक्षी", "N/A"),
                "प्रकरण": json.dumps(details.get("प्रकरण", []), ensure_ascii=False),
                "ठहर": json.dumps(details.get("ठहर", []), ensure_ascii=False),
                "html_file_path": html_file_path
            }
            
            # Save to SQLite
            self.save_to_sqlite(data)
            print(f"{url} - Successfully Scraped and Entered")
            return True
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return False
    
    def scrape_case_details_2062_to_2072(self, url, mudda_type, sal = None, use_saved=True):  # CHANGE 4: Remove output_db parameter
        """Scrape details from a single case URL"""
        try:
            # IMPROVEMENT 16: Check if URL already exists in database
            cursor = self.conn.cursor()
            cursor.execute('SELECT लिङ्क FROM cases WHERE लिङ्क = ?', (url,))
            if cursor.fetchone():
                print(f"URL {url} already exists in database, skipping...")
                return True
            # Get soup using saved HTML or web
            soup = self.return_soup(url, mudda_type, sal, use_saved)
            if not soup:
                print(f"Failed to get content for {url}")
                return False
            
            # Extract basic information
            title_tag = soup.find("h1", class_="post-title")
            decision_title = title_tag.get_text(strip=True).split()[2] if title_tag and len(title_tag.get_text(strip=True).split()) > 2 else "N/A"  # IMPROVEMENT 19: Bounds checking
            
            bhaag = self.get_edition_field(soup, "भाग")
            saal = self.get_edition_field(soup, "साल")
            mahina = self.get_edition_field(soup, "महिना")
            anka = self.get_edition_field(soup, "अंक")
            
            # Extract decision date
            post_meta = soup.find("div", class_="post-meta")
            decision_date = "N/A"
            if post_meta and "फैसला मिति" in post_meta.text:
                try:  # IMPROVEMENT 20: Better error handling for date extraction
                    decision_date = post_meta.text.strip().split("फैसला मिति :")[-1].split("\n")[0].strip().split()[0]
                except IndexError:
                    decision_date = "N/A"
            
            # Extract detailed information
            div_tag = soup.find("div", id="faisala_detail ")
            details = {}
            
            if div_tag:
                tags = div_tag.find_all(['h1', 'p'])
                n = len(tags)
                ind = 0
                temp_ind_32 = ind
                KEYWORDS_2 = ["प्रकरण नं.", "(प्रकरण नं", "(प्रकारण नं.", "९प्रकरण नं।", "(प्रकरण", "(प्र नं.","( प्र. नं","(प्र.नं", "(प्र. नं", "( प्रकरण नं.", "( प्रकरणन", "( प्र.नं.", "( प्र . नं .", "( प ्र . नं .", "(प्ररकण नं.", "(प्रकराण नं."]
                KEYWORDS_3 = ["निवेदक", "वादी", "पुनरावेदक", "निबेदक", "पुनरावदेक", "निवेदिका", "निवेदीका", "निवदेक", "न ि वेदक ः", "नि वेदक ः", "पुनरावेदन", "पुनरवेदिका", "पुनरावेदिका", "पुनरावेदीका", "बादि", "पुनराबेदक", "प्रतिबादी", "पुनरावेक", "अपीलाट", "निवेदनक", "उजुरवाला", "अपिलबाट", "अपिलाट"]
                KEYWORDS_4 = ["विपक्षी", "प्रतिवादी", "प्रत्यर्थी", "बिपक्षी", "विपक्षी ः", "पिपक्षी", "प्रत्यार्थी", "विपक्ष", "रेस्पोण्डेण्ट", "रेस्पोन्डेन्ट", "प्रत्यथी"]
                KEYWORDS_5 = ["विषय", "मुद्दा", "बिषय", "मूद्दा", "मुद्द", "मद्दा", "विपक्ष", "मुद्धा", "मुद् दा"]
                KEYWORDS_6 = ["अदालत", "इजलास", "इजालास", "इजलाश", "बेञ्च"]
                KEYWORDS_7 = ["आदेश", "फैसला", "फैसलमा", "निर्णय", "फै सला", "मुद्दा"]
                KEYWORDS_8 = ["न्यायाधीश", "माननीय", "न्यायधीश", "न्यायाधीस", "न्ययाधीश", "न्यायाधिश", "न्यायाधी", "न्यानायधीश", "नयायाधीश", "न्यायाधधिश", "नयाधश"]
                KEYWORDS_9 = [ "विरूद्ध", "बिरूद्ध", "विरुद्ध", "बिरुद्ध"]
                KEYWORDS_10 = ["AP", "FN", "RE", "RI", "LE", "RV", "NF", "CI", "CR", "RC", "SA", "MS", "ND", "RB", "CF", "DF", "RF", "WO", "WH", "WS", "WF", "WC", "CC", "EC"]
                
                # Extract court information
                temp_ijlash = ""
                while(ind < n):
                    #text = p_tags[ind].get_text(strip=True)
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if text:
                        if any(kw == text for kw in KEYWORDS_6):
                            if "निर्णय नं." not in temp_ijlash:
                                details["अदालत / इजलास"] = temp_ijlash
                            ind+=1
                            break
                        elif any(kw in text for kw in KEYWORDS_6):
                            details["अदालत / इजलास"] = text
                            ind+=1
                            text_2 = tags[ind].get_text(separator=' ', strip=True)
                            if any(kw in text_2 for kw in KEYWORDS_8) == False:
                                details["अदालत / इजलास"] = text +" "+ text_2
                                ind+=1
                            break
                        elif any(kw in text for kw in KEYWORDS_8):
                            if "निर्णय नं." not in temp_ijlash:
                                details["अदालत / इजलास"] = temp_ijlash
                            break
                        temp_ijlash = text
                    ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Extract judges
                judges = []
                faisla_miti_before_case_no = False
                subject_before_case_no = False
                while(ind < n):
                    #text = p_tags[ind].get_text(strip=True)
                    text = tags[ind].get_text(separator=' ', strip=True)
                    if text:
                        if any(kw in text for kw in KEYWORDS_8):
                            judges.append(text)
                        else:
                            details["न्यायाधीश"] = judges
                            if any(text.startswith(kw) for kw in KEYWORDS_7) and ("मिति" in text or "मिती" in text):
                                details["आदेश मिति"] = text
                                ind+=1
                                faisla_miti_before_case_no = True
                            elif any(kw in text for kw in KEYWORDS_10):
                                details["केस_नम्बर"] = text
                            elif any(kw2 in text for kw2 in KEYWORDS_3) == False and any(kw2 in text for kw2 in KEYWORDS_5) == False:
                                if text!="फैसला":
                                    details["केस_नम्बर"] = text
                                else:
                                    ind+=1
                                    details["केस_नम्बर"] = tags[ind].get_text(separator=' ', strip=True)
                                ind+=1
                            break
                    ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind

                # Standard case structure

                if faisla_miti_before_case_no:
                    while ind < n:
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if text:
                            if any(kw in text for kw in KEYWORDS_10):
                                details["केस_नम्बर"] = text
                            elif any(text.startswith(kw) for kw in KEYWORDS_5):
                                subject_before_case_no = True
                                details["विषय"] = text
                            else:
                                details["केस_नम्बर"] = text
                            ind+=1
                            break
                        ind+=1
                else:
                    while ind < n:
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if text:
                            if any(text.startswith(kw) for kw in KEYWORDS_7) and ("मिति" in text or "मिती" in text):
                                details["आदेश मिति"] = text
                                ind+=1
                                break
                            if any(text.startswith(kw) for kw in KEYWORDS_2) or "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                                ind = temp_ind_32
                                break
                        ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind

                if subject_before_case_no:
                    while ind < n:
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if text:
                            details["केस_नम्बर"] = text
                            ind+=1
                            break
                        ind+=1
                else: 
                    while ind < n:
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if text:
                            if any(text.startswith(kw) for kw in KEYWORDS_5):
                                details["विषय"] = text
                                ind+=1
                                break
                            if any(kw in text for kw in KEYWORDS_3):
                                ind = temp_ind_32
                                break
                        ind+=1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                temp_ind_64 = ind
                count_how_many = 0
                
                while temp_ind_64 < n:
                    text = tags[temp_ind_64].get_text(separator=' ', strip=True)
                    if text and any(kw == text for kw in KEYWORDS_9):
                        count_how_many += 1
                    if any(text.startswith(kw) for kw in KEYWORDS_2):
                        break
                    temp_ind_64+=1


                if count_how_many > 1:
                    case_no = []
                    appellant = []
                    opposition = []
                    while count_how_many > 0:
                        while ind < n:
                        #text = p_tags[ind].get_text(strip=True)
                            text = tags[ind].get_text(separator=' ', strip=True)
                            if any(kw in text for kw in KEYWORDS_3):
                                if any(kw2 == text for kw2 in KEYWORDS_3):
                                    ind += 1
                                    text = tags[ind].get_text(separator=' ', strip=True)
                                appellant.append(text)
                                ind+=1
                                break
                            ind+=1
                    
                        if ind >= n:
                            ind = temp_ind_32
                        else:
                            temp_ind_32 = ind
                    
                        while ind < n:
                                #text = p_tags[ind].get_text(strip=True)
                            text = tags[ind].get_text(separator=' ', strip=True)
                            if any(kw in text for kw in KEYWORDS_4):
                                if any(kw2 == text for kw2 in KEYWORDS_4):
                                    ind += 1
                                    text = tags[ind].get_text(separator=' ', strip=True)
                                opposition.append(text)
                                ind+=1
                                break
                            ind+=1
                            
                        if ind >= n:
                            ind = temp_ind_32
                        else:
                            temp_ind_32 = ind

                        count_how_many-=1
                        
                    temp_ind_128 = 0
                    
                    while temp_ind_128 < n:
                        text = tags[temp_ind_128].get_text(separator=' ', strip=True)
                        if text:
                            if any(kw in text for kw in KEYWORDS_10):
                                case_no.append(text)
                            if any(text.startswith(kw) for kw in KEYWORDS_2) or any(kw == text for kw in KEYWORDS_7):
                                break
                        temp_ind_128 += 1

                    details["केस_नम्बर"] = case_no
                    details["निवेदक"] = appellant
                    details["विपक्षी"] = opposition

                else:
                    while ind < n:
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if any(kw in text for kw in KEYWORDS_3):
                            if any(kw2 == text for kw2 in KEYWORDS_3):
                                ind += 1
                                text = tags[ind].get_text(separator=' ', strip=True)
                            details["निवेदक"] = text
                            ind+=1
                            break
                        ind+=1 

                    if ind >= n:
                        ind = temp_ind_32
                    else:
                        temp_ind_32 = ind

                    while ind < n:
                        #text = p_tags[ind].get_text(strip=True)
                        text = tags[ind].get_text(separator=' ', strip=True)
                        if any(kw in text for kw in KEYWORDS_4):
                            if any(kw2 == text for kw2 in KEYWORDS_4):
                                ind += 1
                                text = tags[ind].get_text(separator=' ', strip=True)
                            details["विपक्षी"] = text
                            ind+=1
                            break
                        ind+=1

                    if ind >= n:
                        ind = temp_ind_32
                    else:
                        temp_ind_32 = ind
                
                
                # Extract prakarans and tahar
                prakarans = []
                prev = ""


                tahar = []
                temp_flag_tahar = False

                for tag in tags[ind:]:
                    text = tag.get_text(separator=' ', strip=True)
                    if text:
                        #if "§" in text or any(kw in text for kw in KEYWORDS_2):
                            #prakarans.append(text)
                        if "§" in text or any(text.startswith(kw) for kw in KEYWORDS_2) or "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                            if any(text.startswith(kw) for kw in KEYWORDS_2):
                                if prev:  # IMPROVEMENT 23: Only append if prev has content
                                    prakarans.append(prev)
                                prakarans.append(text)
                                prev = ""
                            if "§" in text:
                                prakarans.append(text)
                            if "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                                if not prakarans:
                                    prakarans.append(prev)
                        else:
                            prev = prev + " " + text if prev else text
                        
                        if text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                            temp_flag_tahar = True
                            tahar.append(text)

                    # Process list items
                    next_sib = tag.find_next_sibling()
                    while next_sib and next_sib.name in ['ul', 'ol']:
                        for li in next_sib.find_all('li'):
                            li_text = li.get_text(separator=' ', strip=True)
                            if li_text:
                                if any(li_text.startswith(kw) for kw in KEYWORDS_2):
                                    if prev:  # IMPROVEMENT 24: Only append if prev has content
                                        prakarans.append(prev)
                                    prakarans.append(li_text)
                                    prev = ""
                                else:
                                    prev = prev + " " + li_text if prev else li_text
                                if li_text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                                    temp_flag_tahar = True
                                    tahar.append(li_text)
                        next_sib = next_sib.find_next_sibling()
                
                details["प्रकरण"] = prakarans
                details["ठहर"] = tahar

            # Get HTML file path
            html_file_path = ""
            if mudda_type and sal:
                filename = self.generate_html_filename(url, mudda_type, sal)
                html_file_path = os.path.join(self.html_folder, filename)
            
            # Combine all data, handling lists and strings appropriately
            data = {
                "लिङ्क": url,
                "निर्णय नं.": decision_title,
                "भाग": bhaag or "N/A",  # IMPROVEMENT 25: Handle None values
                "मुद्दाको किसिम": mudda_type,
                "साल": saal or "N/A",
                "महिना": mahina or "N/A",
                "अंक": anka or "N/A",
                "फैसला मिति": f"'{decision_date}'",
                "अदालत / इजलास": details.get("अदालत / इजलास", "N/A"),
                "न्यायाधीश": json.dumps(details.get("न्यायाधीश", []), ensure_ascii=False),
                "आदेश मिति": details.get("आदेश मिति", "N/A"),
                "केस_नम्बर": json.dumps(details.get("केस_नम्बर", []), ensure_ascii=False) if isinstance(details.get("केस_नम्बर"), list) else details.get("केस_नम्बर", "N/A"),
                "विषय": details.get("विषय", "N/A"),
                "निवेदक": json.dumps(details.get("निवेदक", []), ensure_ascii=False) if isinstance(details.get("निवेदक"), list) else details.get("निवेदक", "N/A"),
                "विपक्षी": json.dumps(details.get("विपक्षी", []), ensure_ascii=False) if isinstance(details.get("विपक्षी"), list) else details.get("विपक्षी", "N/A"),
                "प्रकरण": json.dumps(details.get("प्रकरण", []), ensure_ascii=False),
                "ठहर": json.dumps(details.get("ठहर", []), ensure_ascii=False),
                "html_file_path": html_file_path
            }
            
            # Save to SQLite
            self.save_to_sqlite(data)
            print(f"{url} - Successfully Scraped and Entered")
            return True
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return False

    def scrape_case_details_2073_to_2080_and_beyond(self, url, mudda_type, sal = None, use_saved=True):
        """Scrape details from a single case URL"""
        try:
            r = requests.get(url, timeout=15)
            if r.status_code != 200:
                print(f"Failed to retrieve {url}, Status code: {r.status_code}")
                return False

            # Get soup using saved HTML or web
            soup = self.return_soup(url, mudda_type, sal, use_saved)
            if not soup:
                print(f"Failed to get content for {url}")
                return False
            
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
                tags = div_tag.find_all(['h1', 'p'])
                n = len(tags)
                ind = 0
                temp_ind_32 = ind
                #KEYWORDS_2 = ["(प्रकरण नं", "(प्रकारण नं.", "९प्रकरण नं।", "(प्रकरण", "(प्र.नं."]

                KEYWORDS_2 = ["प्रकरण नं.", "(प्रकरण नं", "(प्रकारण नं.", "९प्रकरण नं।", "(प्रकरण", "(प्र नं.","( प्र. नं","(प्र.नं", "(प्र. नं", "( प्रकरण नं.", "( प्रकरणन", "( प्र.नं.", "( प्र . नं .", "( प ्र . नं .", "(प्ररकण नं.", "(प्रकराण नं."]
                KEYWORDS_3 = ["निवेदक", "वादी", "पुनरावेदक", "निबेदक", "पुनरावदेक", "निवेदिका", "निवेदीका", "निवदेक", "न ि वेदक ः", "नि वेदक ः", "पुनरावेदन", "पुनरवेदिका", "पुनरावेदिका", "पुनरावेदीका", "बादि", "पुनराबेदक", "प्रतिबादी", "पुनरावेक", "अपीलाट", "निवेदनक", "उजुरवाला", "अपिलबाट", "अपिलाट"]
                KEYWORDS_4 = ["विपक्षी", "प्रतिवादी", "प्रत्यर्थी", "बिपक्षी", "विपक्षी ः", "पिपक्षी", "प्रत्यार्थी", "विपक्ष", "रेस्पोण्डेण्ट", "रेस्पोन्डेन्ट", "प्रत्यथी"]
                KEYWORDS_5 = ["विषय", "मुद्दा", "बिषय", "मूद्दा", "मुद्द", "मद्दा", "विपक्ष", "मुद्धा", "मुद् दा"]
                KEYWORDS_6 = ["अदालत", "इजलास", "इजालास", "इजलाश", "बेञ्च"]
                KEYWORDS_7 = ["आदेश", "फैसला", "फैसलमा", "निर्णय", "फै सला", "मुद्दा"]
                KEYWORDS_8 = ["न्यायाधीश", "माननीय", "न्यायधीश", "न्यायाधीस", "न्ययाधीश", "न्यायाधिश", "न्यायाधी", "न्यानायधीश", "नयायाधीश", "न्यायाधधिश", "नयाधश"]
                KEYWORDS_9 = [ "विरूद्ध", "बिरूद्ध", "विरुद्ध", "बिरुद्ध"]
                KEYWORDS_10 = ["AP", "FN", "RE", "RI", "LE", "RV", "NF", "CI", "CR", "RC", "SA", "MS", "ND", "RB", "CF", "DF", "RF", "WO", "WH", "WS", "WF", "WC", "CC", "EC"]
                # Extract court information
                while ind < n:
                    text = tags[ind].get_text(strip=True)
                    if text and any(kw in text for kw in KEYWORDS_6):
                        details["अदालत / इजलास"] = text
                        ind += 1
                        break
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Extract judges
                judges = []
                while ind < n:
                    text = tags[ind].get_text(strip=True)
                    if text:
                        if any(kw in text for kw in KEYWORDS_8):
                            judges.append(text)
                        if any(text.startswith(kw) for kw in KEYWORDS_7) and ("मिति" in text or "मिती" in text):
                            details["न्यायाधीश"] = judges
                            details["आदेश मिति"] = text
                            ind += 1
                            break
                    ind += 1

                if ind >= n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Extract case details
                bisaya_before_kas_no = False
                while ind < n:
                    text = tags[ind].get_text(strip=True)
                    if text:
                        if any(kw in text for kw in KEYWORDS_5):
                            bisaya_before_kas_no = True
                            details["विषय"] = text
                            ind += 1
                            break
                        details["केस_नम्बर"] = text
                        break
                    ind += 1

                if ind > n:
                    ind = temp_ind_32
                else:
                    temp_ind_32 = ind
                
                # Handle different case structures
                if bisaya_before_kas_no:
                    case_no = []
                    appellant = []
                    opposition = []
                    temp_flag = True
                    
                    while temp_flag and ind < n:
                        # Extract case number
                        while ind < n:
                            text = tags[ind].get_text(strip=True)
                            if text:
                                case_no.append(text)
                                ind += 1
                                break
                            ind += 1
                        
                        # Extract appellant
                        while ind < n:
                            text = tags[ind].get_text(strip=True)
                            if any(kw in text for kw in KEYWORDS_3):
                                if any(kw2 == text for kw2 in KEYWORDS_3):
                                    ind+=1
                                    text = tags[ind].get_text(strip=True)
                                appellant.append(text)
                                ind += 1
                                break
                            ind += 1
                        
                        # Extract opposition
                        while ind < n:
                            text = tags[ind].get_text(strip=True)
                            if any(kw in text for kw in KEYWORDS_4):
                                if any(kw2 == text for kw2 in KEYWORDS_4):
                                    ind += 1
                                    text = tags[ind].get_text(strip=True)
                                opposition.append(text)
                                ind += 1
                                break
                            ind += 1
                        
                        # Check for end condition
                        temp_ind = ind
                        for tag in tags[temp_ind:]:
                            text = tag.get_text(strip=True)
                            if any(kw in text for kw in KEYWORDS_2):
                                temp_flag = False
                                details["केस_नम्बर"] = case_no
                                details["निवेदक"] = appellant
                                details["विपक्षी"] = opposition
                                break
                            elif any(kw == text for kw in KEYWORDS_9):
                                break

                    if ind >= n:
                        ind = temp_ind_32
                    else:
                        temp_ind_32 = ind
                else:
                    # Standard case structure
                    while ind < n:
                        text = tags[ind].get_text(strip=True)
                        if any(kw in text for kw in KEYWORDS_5):
                            details["विषय"] = text
                            ind += 1
                            break
                        ind += 1

                    if ind >= n:
                        ind = temp_ind_32
                    else:
                        temp_ind_32 = ind
                    
                    while ind < n:
                        text = tags[ind].get_text(strip=True)
                        if any(kw in text for kw in KEYWORDS_3):
                            if any(kw2 == text for kw2 in KEYWORDS_3):
                                ind+=1
                                text = tags[ind].get_text(strip=True)
                            details["निवेदक"] = text
                            ind += 1
                            break
                        ind += 1

                    if ind >= n:
                        ind = temp_ind_32
                    else:
                        temp_ind_32 = ind
                    
                    while ind < n:
                        text = tags[ind].get_text(strip=True)
                        if any(kw in text for kw in KEYWORDS_4):
                            if any(kw2 == text for kw2 in KEYWORDS_4):
                                ind+=1
                                text = tags[ind].get_text(strip=True)
                            details["विपक्षी"] = text
                            ind += 1
                            break
                        ind += 1

                    if ind >= n:
                        ind = temp_ind_32
                    else:
                        temp_ind_32 = ind
                
                # Clean up extracted text
                # self.clean_extracted_details(details, bisaya_before_kas_no)
                
                # Extract prakarans and tahar
                prakarans = []
                prev = ""
                tahar = []
                temp_flag_tahar = False

                for tag in tags[ind:]:
                    text = tag.get_text(separator=' ', strip=True)
                    if text:
                        #if "§" in text or any(kw in text for kw in KEYWORDS_2):
                            #prakarans.append(text)
                        if "§" in text or any(text.startswith(kw) for kw in KEYWORDS_2) or "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                            if any(text.startswith(kw) for kw in KEYWORDS_2):
                                if prev:  # IMPROVEMENT 23: Only append if prev has content
                                    prakarans.append(prev)
                                prakarans.append(text)
                                prev = ""
                            if "§" in text:
                                prakarans.append(text)
                            if "फैसला"==text or "आदेश"==text or "फैसलाः"==text:
                                if not prakarans:
                                    prakarans.append(prev)
                        else:
                            prev = prev + " " + text if prev else text
                        
                        if text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                            temp_flag_tahar = True
                            tahar.append(text)

                    # Process list items
                    next_sib = tag.find_next_sibling()
                    while next_sib and next_sib.name in ['ul', 'ol']:
                        for li in next_sib.find_all('li'):
                            li_text = li.get_text(separator=' ', strip=True)
                            if li_text:
                                if any(li_text.startswith(kw) for kw in KEYWORDS_2):
                                    if prev:  # IMPROVEMENT 24: Only append if prev has content
                                        prakarans.append(prev)
                                    prakarans.append(li_text)
                                    prev = ""
                                else:
                                    prev = prev + " " + li_text if prev else li_text
                                if li_text in ["फैसला", "आदेश", "फैसलाः"] or temp_flag_tahar:
                                    temp_flag_tahar = True
                                    tahar.append(li_text)
                        next_sib = next_sib.find_next_sibling()
                
                details["प्रकरण"] = prakarans
                details["ठहर"] = tahar

            # Get HTML file path
            html_file_path = ""
            if mudda_type and sal:
                filename = self.generate_html_filename(url, mudda_type, sal)
                html_file_path = os.path.join(self.html_folder, filename)
            
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
                "अदालत / इजलास": details.get("अदालत / इजलास", "N/A"),
                "न्यायाधीश": json.dumps(details.get("न्यायाधीश", []), ensure_ascii=False),
                "आदेश मिति": details.get("आदेश मिति", "N/A"),
                "केस_नम्बर": json.dumps(details.get("केस_नम्बर", []), ensure_ascii=False) if isinstance(details.get("केस_नम्बर"), list) else details.get("केस_नम्बर", "N/A"),
                "विषय": details.get("विषय", "N/A"),
                "निवेदक": json.dumps(details.get("निवेदक", []), ensure_ascii=False) if isinstance(details.get("निवेदक"), list) else details.get("निवेदक", "N/A"),
                "विपक्षी": json.dumps(details.get("विपक्षी", []), ensure_ascii=False) if isinstance(details.get("विपक्षी"), list) else details.get("विपक्षी", "N/A"),
                "प्रकरण": json.dumps(details.get("प्रकरण", []), ensure_ascii=False),
                "ठहर": json.dumps(details.get("ठहर", []), ensure_ascii=False),
                "html_file_path": html_file_path
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
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO cases (
                    लिङ्क, निर्णय_नं, भाग, मुद्दाको_किसिम, साल, महिना, अंक, फैसला_मिति,
                    अदालत_वा_इजलास, न्यायाधीश, आदेश_मिति, केस_नम्बर, विषय, निवेदक, विपक्षी, 
                    प्रकरण, ठहर, html_file_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data["लिङ्क"], data["निर्णय नं."], data["भाग"], data["मुद्दाको किसिम"],
                data["साल"], data["महिना"], data["अंक"], data["फैसला मिति"],
                data["अदालत / इजलास"], data["न्यायाधीश"], data["आदेश मिति"], data["केस_नम्बर"],
                data["विषय"], data["निवेदक"], data["विपक्षी"], data["प्रकरण"], data["ठहर"],
                data["html_file_path"]
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            raise
    
    def save_failed_links(self, failed_links, mudda_type, sal, error_msg="Unknown error"):
        """Save failed links to SQLite database"""
        if failed_links:
            cursor = self.conn.cursor()
            for link in failed_links:
                try:
                    cursor.execute('''
                        INSERT INTO failed_links (मुद्दाको_किसिम, साल, लिङ्क, error_message, retry_count)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (mudda_type, sal, link, error_msg, 1))
                except sqlite3.Error as e:
                    print(f"Error saving failed link {link}: {e}")
            try:
                self.conn.commit()
            except sqlite3.Error as e:
                print(f"Error committing failed links: {e}")

    def test_single_link(self, url, mudda_type=None, sal=None, use_saved=True):
        """Test scraping a single link"""
        print(f"Testing single link: {url}")
        
        # If mudda_type and sal not provided, try to extract from existing data or filename
        if not mudda_type or not sal:
            cursor = self.conn.cursor()
            cursor.execute('SELECT मुद्दाको_किसिम, साल FROM cases WHERE लिङ्क = ?', (url,))
            result = cursor.fetchone()
            if result:
                mudda_type, sal = result
                print(f"Found existing data: mudda_type={mudda_type}, sal={sal}")
        
        if not mudda_type or not sal:
            print("Warning: mudda_type and sal not provided and couldn't be determined from existing data")
            print("Using generic scraping without HTML file management")
        
        success = self.scrape_case_details_generic(url, mudda_type, sal, use_saved)
        if success:
            print("✓ Successfully scraped and saved to database")
        else:
            print("✗ Failed to scrape")
        
        return success

    def test_saved_html_files(self, mudda_type=None, sal=None, limit=None):
        """Test scraping from saved HTML files"""
        html_files = self.get_saved_html_files_by_criteria(mudda_type, sal)
        
        if not html_files:
            print("No saved HTML files found matching criteria")
            return
        
        print(f"Found {len(html_files)} saved HTML files")
        
        if limit:
            html_files = html_files[:limit]
            print(f"Testing first {limit} files")
        
        successful_count = 0
        failed_count = 0
        
        for html_file in html_files:
            file_mudda_type, file_sal, link_number = self.extract_info_from_filename(html_file)
            
            if not file_mudda_type or not file_sal:
                print(f"Could not extract info from filename: {html_file}")
                failed_count += 1
                continue
            
            # Reconstruct URL (this is a simplified approach)
            url = f"https://nkp.gov.np/full_detail/{link_number}"
            
            print(f"Testing {html_file} -> {file_mudda_type}, {file_sal}")
            
            success = self.scrape_case_details_generic(url, file_mudda_type, file_sal, use_saved=True)
            
            if success:
                successful_count += 1
            else:
                failed_count += 1
        
        print(f"\nTest Results:")
        print(f"✓ Successful: {successful_count}")
        print(f"✗ Failed: {failed_count}")
        print(f"Total: {len(html_files)}")

    def run_scraper(self, mudda_type, sal, use_saved=True):
        """Main method to run the scraper"""
        print(f"Starting scraper for mudda_type: {mudda_type}, sal: {sal}")
        print(f"Using database: {self.output_db}")
        print(f"HTML folder: {self.html_folder}")
        print(f"Use saved HTML files: {use_saved}")
        
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
        case_urls = self.get_all_pages(search_url, mudda_type, sal, use_saved)
        
        if not case_urls:
            print("No case URLs found!")
            return
        
        print(f"Found {len(case_urls)} case URLs to scrape")
        
        # Scrape each case
        successful_count = 0
        failed_links = []
        
        for i, url in enumerate(case_urls, 1):
            print(f"Processing {i}/{len(case_urls)}: {url}")
            
            success = self.scrape_case_details_generic(url, mudda_type, sal, use_saved)
            if success:
                successful_count += 1
            else:
                failed_links.append(url)
            
            # Add delay between requests only if downloading from web
            if not use_saved:
                time.sleep(2)
        
        # Retry failed links once
        if failed_links:
            print(f"\nRetrying {len(failed_links)} failed links...")
            still_failed = []
            
            for i, url in enumerate(failed_links, 1):
                print(f"Retrying {i}/{len(failed_links)}: {url}")
                
                success = self.scrape_case_details_generic(url, mudda_type, sal, use_saved=False)  # Force web download on retry
                if success:
                    successful_count += 1
                else:
                    still_failed.append(url)
                
                time.sleep(2)
            
            # Save permanently failed links
            if still_failed:
                self.save_failed_links(still_failed, mudda_type, sal, "Failed after retry")
                
                print(f"\nFinal Results:")
                print(f"Total links found: {len(case_urls)}")
                print(f"Successfully scraped: {successful_count}")
                print(f"Failed to scrape: {len(still_failed)}")
                
                if still_failed:
                    print(f"Failed links saved to database: failed_links table")
        else:
            print(f"\nResults:")
            print(f"Total links found: {len(case_urls)}")
            print(f"Successfully scraped: {successful_count}")
        
        print(f"Scraped data saved to SQLite database: {self.output_db}")

    def close(self):
        """Explicitly close the database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()

    def __del__(self):
        """Close SQLite connection when the object is destroyed"""
        self.close()


def create_parser():
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(
        description="Legal Case Scraper for Nepal Kanoon Patrika",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape specific mudda_type and year
  python app.py --mudda_type "दुनियाबादी देवानी" --nepali_year "२०७३" --database_name "app_test_db.db"
  
  # Test a specific link
  python app.py --test_link "https://nkp.gov.np/8035" --mudda_type "दुनियाबादी देवानी" --nepali_year "२०७३"
  
  # Test saved HTML files
  python app.py --test_saved --nepali_year "२०७३" --limit 5
  
  # Use saved HTML files for scraping (faster)
  python app.py --mudda_type "दुनियाबादी देवानी" --nepali_year "२०७३" --use_saved
  
  # List available mudda types
  python app.py --list_mudda_types
        """
    )
    
    parser.add_argument('--mudda_type', type=str, 
                       help='Mudda type (e.g., "दुनियाबादी देवानी")')
    
    parser.add_argument('--nepali_year', type=str,
                       help='Nepali year (e.g., "२०७३")')
    
    parser.add_argument('--database_name', type=str, default='legal_cases_2.db',
                       help='SQLite database filename (default: legal_cases_2.db)')
    
    parser.add_argument('--html_folder', type=str, default='scraped_html',
                       help='Folder to store HTML files (default: scraped_html)')
    
    parser.add_argument('--use_saved', action='store_true',
                       help='Use saved HTML files when available (faster)')
    
    parser.add_argument('--test_link', type=str,
                       help='Test scraping a specific link')
    
    parser.add_argument('--test_saved', action='store_true',
                       help='Test scraping from saved HTML files')
    
    parser.add_argument('--limit', type=int,
                       help='Limit number of files to test (use with --test_saved)')
    
    parser.add_argument('--list_mudda_types', action='store_true',
                       help='List all available mudda types')
    
    return parser


def main():
    """Main function to run the application"""
    parser = create_parser()
    args = parser.parse_args()
    
    # List mudda types if requested
    if args.list_mudda_types:
        temp_scraper = LegalCaseScraper()
        print("Available mudda_type options:")
        for i, option in enumerate(temp_scraper.mudda_type_arr, 1):
            print(f"{i}. {option}")
        temp_scraper.close()
        return
    
    # Create the scraper
    scraper = LegalCaseScraper(
        output_db=args.database_name,
        html_folder=args.html_folder
    )
    
    try:
        # Test single link
        if args.test_link:
            success = scraper.test_single_link(
                args.test_link, 
                args.mudda_type, 
                args.nepali_year,
                use_saved=args.use_saved
            )
            return
        
        # Test saved HTML files
        if args.test_saved:
            scraper.test_saved_html_files(
                mudda_type=args.mudda_type,
                sal=args.nepali_year,
                limit=args.limit
            )
            return
        
        # Regular scraping
        if not args.mudda_type or not args.nepali_year:
            print("Error: --mudda_type and --nepali_year are required for scraping")
            print("Use --help for usage examples")
            return
        
        scraper.run_scraper(
            mudda_type=args.mudda_type,
            sal=args.nepali_year,
            use_saved=args.use_saved
        )
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
