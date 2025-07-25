To use this we can either tap on them like application or we can use the command line

 They need 3 inputs
1. Mudda Type
2. Nepali Year
3. Database Name (Optional cause if now given then it will add a name itself)

Mudda Types:
1. दुनियाबादी देवानी
2. सरकारबादी देवानी
3. दुनियावादी फौजदारी
4. सरकारवादी फौजदारी
5. रिट
6. निवेदन
7. विविध


For command line go to the folder where the app is downloaded then below is an example:
>python Nepal_Kanun_Patrika_Web_Scrapper_from_2050_to_2043.py "दुनियाबादी देवानी" "२०५०" "database_name.db"

Then a sqlite3 database will be created by the name of "database_name" and it will start scrapping all cases from Mudda type: "दुनियाबादी देवानी" and Nepali Year: "२०५०"
