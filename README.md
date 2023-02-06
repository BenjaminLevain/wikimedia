# wikimedia
Project for downloading data from wikimedia and analyse it automatically

## Prerequisite 

* python >= 3.8
* pypark
* command for installing pyspark

```text
 pip install pyspark
 ```
 
## Scripts in the repo

* blacklist_domains_and_pages
  * It contains the list of the unwanted pages we want to remove from our data dowloaded on the web
  * IMPORTANT : It has to be located in the same folder of the other module scripts`
  
* loading_data_wikimedia.py
  * It is the script to execute to load data for a given range and a given path where to save the data
  * It has to be executed in a terminal and it has three parameters
    * First it is the start datetime parameter , writen as a string with this format `%Y-%m-%d %H:%M:%S'`
    * Second it is the end datetime parameter , writen as a string with this format `%Y-%m-%d %H:%M:%S'`
    * Last is the location where you want to save your parquet data
  * Exemple to run it:
    * `python loading_data_wikimedia.py '2022-01-01 00:00:00' '2023-01-01 00:00:00' '/Users/benjaminlevain/Documents/results'`
    
* **analytics_function.py**
  * It contains several functions to answer the business questions asked
  * Each function enables to answer each questions but I would have also could splitting into single tasks functions and apply several function to answer one questions. 
  * I put in a string in the code the philosophy of each functions
  * The time granularity chose was the day but it is debatable. for instance for question 2 , it not idiot to take the week to don't have drop or spike sensitive to the day of the week
    
 ## Written functions
 see [README](WrittenQuestions.md)



