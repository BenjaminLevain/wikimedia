
#### import packages needed
import requests
import sys 
from pyspark.sql.window import Window
from pyspark.sql.functions import desc,row_number,lit
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import datetime as dt 

#####
DATE_TIME_STRING_FORMAT = '%Y-%m-%d %H:%M:%S'

#### gett datetime_list from start and end of the range defined in the parameters of the command line
start_range_date = dt.datetime.strptime(sys.argv[1],
                                   DATE_TIME_STRING_FORMAT)

end_range_date = dt.datetime.strptime(sys.argv[2],
                                   DATE_TIME_STRING_FORMAT)


date_times_list = [start_range_date]
date_time = start_range_date
while date_time < end_range_date:
    date_time += dt.timedelta(hours=1)
    date_times_list.append(date_time)

### Get black_list_pages sdf
spark = SparkSession.builder.appName("test").getOrCreate()
customSchema_black_list = StructType([
        StructField("domain_code", StringType(), True),
        StructField("page_title", StringType(), True)])
black_list_pages_sdf = spark.read.option("sep", " ").option("header", "true").schema(customSchema_black_list).csv("blacklist_domains_and_pages")
black_list_pages_sdf = black_list_pages_sdf.withColumn("is_black_list_pages",lit(True))


### execute the tasks for each hour of the range 
for datetime in date_times_list:
  print("------------scrip is running for ",str(datetime),"------------------")
  
  #1.get the components of the dates as string 
  year =str(datetime.year)
  month =datetime.month
  if month <10:
    month = '0' +str(month)
  else:
    month = str(month)
  day = datetime.day
  if day <10:
    day = '0' +str(day)
  else:
    day = str(day)
  hour = datetime.hour
  if hour <10:
    hour = '0' +str(hour)
  else:
    hour = str(hour)
    
  #2. Download data 
  url_to_load_data =f'https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz'

  ###get path to save the data defined in one of the parameter given into the command line
  core_path = '/Users/benjaminlevain/Documents/results'
  core_path = sys.argv[3]
  path_to_save = f'{core_path}/{year}/{year}-{month}/{year}{month}{day}-{hour}0000.csv'
  path_to_save =f'{core_path}/year={year}/year-month={year}-{month}/{year}{month}{day}/{year}{month}{day}-{hour}0000'

  ####check if the data has been already loaded or not in order to save time if yes 
  try:
    ###read file
    df_data = spark.read.parquet(path_to_save)
    #df_test = spark.read.csv(path_to_save)
    print("Data already loaded for the given date and the given hour")
    continue
  ##if not loaded yes , dowload the data 
  except:
    print("Data NOT already loaded for the given date and the given hour and is curently downloaded")
    ####create spark Session
    spark = SparkSession.builder.appName("test").getOrCreate()

    ####get url name 
    filename = url_to_load_data.split("/")[-1]
    #print(filename)
    spark.sparkContext.addFile(url_to_load_data)

    ##define the schema of the dataFrame
    customSchema = StructType([
        StructField("domain_code", StringType(), True),
        StructField("page_title", StringType(), True),
        StructField("count_views", IntegerType(), True),
        StructField("total_response_size", IntegerType(), True)])

    ###read file
    df = spark.read.option("sep", " ").option("header", "true").schema(customSchema).csv("file://" + SparkFiles.get(filename))

    ###filter blacklist pages
    df= (df
    .join(black_list_pages_sdf, ["domain_code", "page_title"], "leftouter")
    .where(black_list_pages_sdf["is_black_list_pages"].isNull())
    .drop(black_list_pages_sdf["is_black_list_pages"]))

    ####3.rank By domain thanks to a window funtion
    windowSpec  = Window.partitionBy("domain_code").orderBy(desc("count_views"))
    df = df.withColumn("rank",row_number().over(windowSpec))
    #print(df.count())

    ####select only pages which are in the top 25 for a given pages sorted by domain and by rank
    df = df.where("rank <= 25").orderBy("domain_code","rank")
    #print(df.count())
    #### add year month date and hour
    df = df.withColumn("year",lit(year)).withColumn("month",lit(month)).withColumn("day",lit(day)).withColumn("hour",lit(hour)).withColumn("date",lit(datetime))


    ###4. save data frame 
    # Save file local folder and ignore it if file is already existing
    #df.write.format('csv').option('header',True).mode('ignore').option('sep',',').save(path_to_save)
    df.write.mode("overwrite").parquet(path_to_save)
