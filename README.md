covid stats
Hi All, this project collects and cleans coronavirus related data from GOV.UK, Worldometer and Citymapper
I have put my own front end on it in PowerBI, but all the data prep has been done in python
#Prerequisites: DB Browser (for SQLite3), Excel and all the packages mentioned in requirements.txt file
These stats are a combination of web scraped data and my own derived data based on these scrapes/apis
The output is a SQL databse due to the size.

#pip install requirements.txt file to get additional packages #To run - download the whole folder and execute the run.py 
Explanations of the code are in the individual files (not run.py)

#movement.py no longers runs as citymapper depreciated the api, to run the rest of the script simply comment out line 55 to 61 in the "run.py file"

