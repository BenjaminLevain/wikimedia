## What additional things would you want to operate this application in a production setting?
 -  I would have structured my code into modules and functions to increase maintainability and readability.
 -  I could have also added trackers on errors to debug it more easily and also some loggers to follow that the application is running normally. (I did it with print during my development of the script)
 -  Add a unit test repo with a unit test to test my functions

## What might change about your solution if this application needed to run automatically for each hour of the day?
 -  First, of course, I would need to add a scheduler tool to run the job automatically every hour.
 -  Then, like already said above, I would really need to implement good trackers, loggers,   error exceptions and other alerting systems to follow that the job is correctly executed
-  Optimize the code for scaling running. So be sure that the execution doesn’t take to much time 

## How would you test this application?
 -  The first thing as mentioned before is unit testing.  Implement for single functions of your module a unit test 
 -  Then we need to do of course integration testing in order to assure that all the pipeline of the application is working well. (This part I know that we have to do it but I have never implemented it myself)
-  And The third test I think about is performance testing to be sure that the application is scalable 

## How you’d improve on this application design?

 -  Modularize as possible your application. A module for each component of the application in order to maintain,  debug and understand how the app works easily
 -  It has already been said above but testing the most as possible

##  How would you answer the questions Q1, Q2 and Q3 in the technical test section using SQL and your schema ? Write down the SQL queries for it.
- The schema of the data base is the following one (taken from my code written in pyspark ) 
```customSchema_for_saving = StructType([
StructField("domain_code", StringType(), True),
StructField("page_title", StringType(), True),
StructField("count_views", IntegerType(), True),
StructField("total_response_size", IntegerType(), True),
StructField("year", StringType(), True),
StructField("month", StringType(), True),
StructField("date", StringType(), True),
StructField("hour", StringType(), True),
StructField("datetime", TimestampType(), True)],
)
```
- But I could have also written it in a SQL syntax and with name more precise 
```
CREATE TABLE PageViewsCount (
domain_page_title_date_id INT PRIMARY KEY, (concatenation of the domain ,the page_title and the date) =⇒ added from the pyspark version
domain_code VARCHAR(50) NOT NULL,
page_title VARCHAR(50) NOT NULL,
datetime DATETIME NOT NULL,
year VARCHAR(50) NOT NULL,
month VARCHAR(50) NOT NULL,
day_number VARCHAR(50) NOT NULL,
hour VARCHAR(50) NOT NULL,
count_views INT NOT NULL,
total_response_size INT NOT NULL,
)
```
- SQL REQUEST for Q1 
```
SELECT 
domain_code,
page_title,
MIN_COUNT_VIEWS_OVER_THE_YEAR,
MAX_COUNT_VIEWS_OVER_THE_YEAR,
MAX_DELTA_COUNT_VIEWS_OVER_THE_YEAR,
FROM
(SELECT
domain_code,
page_title,
MAX() OVER (PARTITION BY domain_code,page_title ORDER BY nb_views_by_day)
) AS MAX_COUNT_VIEWS_OVER_THE_YEAR,
MIN() OVER (PARTITION BY domain_code,page_title ORDER BY nb_views_by_day) AS MIN_COUNT_VIEWS_OVER_THE_YEAR
MAX() OVER (PARTITION BY domain_code,page_title ORDER BY nb_views_by_day
) - MIN() OVER (PARTITION BY domain_code,page_title ORDER BY nb_views_by_day AS MAX_DELTA_COUNT_VIEWS_OVER_THE_YEAR
FROM
(SELECT CAST(datetime as DATE) as date , domain_code , page_title, sum(count_views) as nb_views_by_day 
FROM PageViewsCount
GROUP BY CAST(datetime as DATE),page_title,domain_code) as table 1 
) as table 2
GROUP BY domain_code,page_title,MAX_DELTA_COUNT_VIEWS_OVER_THE_YEAR,MIN_COUNT_VIEWS_OVER_THE_YEAR,MAX_COUNT_VIEWS_OVER_THE_YEAR
ORDER BY  MAX_DELTA_COUNT_VIEWS_OVER_THE_YEAR DESC
LIMIT 10
```
- SQL REQUEST for Q2 
```
SELECT 
domain_code,
page_title,
ROUND ( 100 * (nb_views_by_day - nb_views_by_day) / (nb_views_by_day ),1) as Variation_rate,
ABS(ROUND ( 100 * (nb_views_by_day - nb_views_by_day) / (nb_views_by_day ),1)) as absolute_variation_rate
FROM
(SELECT 
domain_code,
page_title,
nb_views_by_day
LAG(nb_views_by_day) OVER (PARTITION BY domain_code,page_title ORDER BY date
) as nb_views_by_day_of_previous_day
FROM
(SELECT  CAST(datetime as DATE) as date , domain_code , page_title, sum(count_views) as nb_views_by_day 
FROM PageViewsCount
WHERE domain_code CONTAINS ‘FR’  
GROUP BY CAST(datetime as DATE),page_title,domain_code) as table 1 
WHERE nb_views_by_day >10 #(optional) ) as table_2
ORDER BY Variation_rate DESC
LIMIT 10
```
- SQL REQUEST for Q3 
```
SELECT 
domain_code,
page_title_cleaned,
date,
ROUND ( 100 * (nb_views_by_day - nb_views_by_day) / (nb_views_by_day ),1) as Variation_rate,
ABS(ROUND ( 100 * (nb_views_by_day - nb_views_by_day) / (nb_views_by_day ),1)) as absolute_variation_rate
FROM
(SELECT 
domain_code,
page_title_cleaned,
date,
sum(count_views) as nb_views_by_day
FROM
(SELECT  
CAST(datetime as DATE) as date ,
domain_code ,
page_title, 
count_views ,
LOWER(REGEXP_REPLACE(page_title,’[:_]’, ‘’)) as page_title_cleaned
FROM PageViewsCount
WHERE domain_code CONTAINS ‘FR’  ) as table 1 
WHERE page_title_cleaned CONTAINS ‘coupedumondedefootbal’ AND nb_views_by_day >10    #(2nd where clause is optional) 
GROUP BY date,page_title_cleaned,domain_code) as table_2
ORDER BY Variation_rate DESC (Or ORDER BY date to follow with the time)
```
## Assume that a page can now optionally be part of one or more categories. How would you change your schema to support this ?
- We should add a column category into the schema ( Probably of VARCHAR type)
- The primary id would become concatenation of four fields with the domain_code,the page title , the datetime and the category

## Write a SQL query to compute the top 25 most visited page per category.
- I interpreted the question like it has to be the top 25 by day like in the first question but if not the request below could be simplified by removing the date in the groupby clause
```
SELECT * 
FROM
(SELECT 
date,
category,
RANK() OVER (PARTITION BY date ORDER BY DESC(nb_views_by_day) AS rank_category_for_the_day,
nb_views_by_day
FROM
(SELECT  CAST(datetime as DATE) as date , category, sum(count_views) as nb_views_by_day 
FROM PageViewsCount
GROUP BY CAST(datetime as DATE),category) as table 1 
) as table 2
WHERE rank_category_for_the_day ≤25
```


