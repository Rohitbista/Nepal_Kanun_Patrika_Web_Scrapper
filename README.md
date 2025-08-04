# Nepal Kanoon Patrika Scraper

A Python-based web scraper for extracting legal case details from the Nepal Kanoon Patrika (NKP) website (https://nkp.gov.np/). This tool scrapes case information, saves it to a SQLite database, and stores HTML files for offline processing. It supports various case types and years, with robust error handling and retry mechanisms.

## Features
- Scrapes legal case details including decision number, court, judges, parties, and case details
- Supports multiple case types (मुद्दाको किसिम) and years (Nepali calendar)
- Stores data in a SQLite database
- Saves HTML pages for offline processing
- Handles pagination and retries failed requests
- Includes testing functionality for single URLs and saved HTML files
- Command-line interface with flexible options

## Prerequisites
- Python 3.8 or higher
- pip (Python package manager)
- Internet connection (for initial scraping)
- Sufficient disk space for HTML files and database

## Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/nepal-kanoon-patrika-scraper.git
   cd nepal-kanoon-patrika-scraper
   ```

2. **Create a Virtual Environment** (recommended)
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

   The `requirements.txt` includes:
   - `requests==2.32.3`
   - `beautifulsoup4==4.12.3`
   - `pandas==2.2.3`
   - `sqlite3` (built-in with Python)

## Usage

The scraper is controlled via command-line arguments. Below are the main commands and their usage.

### 1. List Available Case Types
To see all supported `mudda_type` options:
```bash
python Kanun_Patrika_Scraper.py --list_mudda_types
```
Output example:
```
Available mudda_type options:
1. दुनियाबादी देवानी
2. सरकारबादी देवानी
3. दुनियावादी फौजदारी
4. सरकारवादी फौजदारी
5. रिट
6. निवेदन
7. विविध
```

### 2. Scrape Cases
To scrape cases for a specific case type and year:
```bash
python Kanun_Patrika_Scraper.py --mudda_type "दुनियाबादी देवानी" --nepali_year "२०७३" --database_name "legal_cases.db" --html_folder "scraped_html" --use_saved
```
- `--mudda_type`: Case type (must be one of the listed options)
- `--nepali_year`: Year in Nepali numerals (e.g., २०७३)
- `--database_name`: SQLite database file (default: `legal_cases_2.db`)
- `--html_folder`: Folder for HTML files (default: `scraped_html`)
- `--use_saved`: Use saved HTML files when available (faster)

### 3. Test a Single URL
To test scraping a specific case URL:
```bash
python Kanun_Patrika_Scraper.py --test_link "https://nkp.gov.np/full_detail/8035" --mudda_type "दुनियाबादी देवानी" --nepali_year "२०७३" --use_saved
```

### 4. Test Saved HTML Files
To test scraping from saved HTML files:
```bash
python Kanun_Patrika_Scraper.py --test_saved --nepali_year "२०७३" --limit 5
```
- `--limit`: Limit the number of HTML files to test (optional)

### 5. View Help
For a full list of commands and examples:
```bash
python Kanun_Patrika_Scraper.py --help
```

## Output
- **Database**: Case details are stored in a SQLite database (default: `legal_cases_2.db`) in the `cases` table. Failed links are stored in the `failed_links` table.
- **HTML Files**: Raw HTML pages are saved in the specified `html_folder` (default: `scraped_html`) with filenames in the format `mudda_number_year_link_number.html`.
- **Console Output**: Progress and error messages are printed to the console.

## Database Schema
The `cases` table includes the following columns:
- `लिङ्क`: Case URL
- `निर्णय_नं`: Decision number
- `भाग`: Volume
- `मुद्दाको_किसिम`: Case type
- `साल`: Year
- `महिना`: Month
- `अंक`: Issue
- `फैसला_मिति`: Decision date
- `अदालत_वा_इजलास`: Court/Bench
- `न्यायाधीश`: Judges (JSON)
- `आदेश_मिति`: Order date
- `केस_नम्बर`: Case number (JSON or string)
- `विषय`: Subject
- `निवेदक`: Applicant (JSON or string)
- `विपक्षी`: Opponent (JSON or string)
- `प्रकरण`: Case details (JSON)
- `ठहर`: Verdict (JSON)
- `html_file_path`: Path to saved HTML file
- `created_at`: Timestamp

The `failed_links` table includes:
- `मुद्दाको_किसिम`: Case type
- `साल`: Year
- `लिङ्क`: Failed URL
- `error_message`: Error description
- `retry_count`: Number of retries
- `created_at`: Timestamp

## Notes
- The scraper uses different parsing logic for different year ranges (2015–2044, 2045–2050, 2051–2061, 2062–2072, 2073–2080) to handle variations in page structure.
- HTML files are saved to avoid repeated web requests. Use `--use_saved` to prioritize saved files.
- The scraper includes retry logic for failed requests and stores failed links for later analysis.
- Be cautious with frequent web requests to avoid overwhelming the target server. A 2-second delay is added between requests when not using saved files.
- Nepali numerals are converted to English numerals for internal processing.

## Troubleshooting
- **Missing Dependencies**: Ensure all packages in `requirements.txt` are installed.
- **Database Errors**: Check if the database file is accessible and not locked.
- **Network Issues**: Verify internet connectivity for web scraping.
- **Invalid Case Type or Year**: Use `--list_mudda_types` to see valid case types, and ensure the year is in Nepali numerals.

## Contributing
Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/YourFeature`)
3. Commit your changes (`git commit -m 'Add YourFeature'`)
4. Push to the branch (`git push origin feature/YourFeature`)
5. Open a pull request

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact
For issues or questions, please open an issue on GitHub or contact [your-email@example.com].