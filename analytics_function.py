from pyspark.sql.functions import desc,row_number,lit,date_trunc,col,sum,min,max,lag,when,isnull,round,abs

#### CODE for Question 1
def get_pages_with_biggest_increase_decrease_for_a_country(path_to_load_data,country,nb_top_pages):
  """ 
  I make the asumption that we wanted to have a granularity of the day to measure the biggest increase and decrease in terms of volume during the year
  There is a debate on the period to take to study the variation. it could have been hour(s) or several days but I think one day is quite relevant
  """
  ###read file and load raw data frame
  df_data = spark.read.parquet(path_to_load_data)
  
  ##fill na
  df_data = df_data.na.fill(value = 0 , subset =["count_views"])
  
  ####select country on which you want 
  df_data= df_data.filter(col("domain_code").contains(f"{country}"))
  
  #### agg by day because by hour is too volatil ( I chosen day but it could also be week)
  df_data = df_data.withColumn('date', date_trunc(timestamp='date', format='day'))
  df_data_agg_by_day = df_data.groupBy("date","domain_code","page_title").agg(sum("count_views").alias("nb_views_by_day"))
  
  ###get the min and max of views for a given page and a given domain and calculate the delta between both
  """ We could remove the agregation by domain_code if we just want to know which page in general had the huge increase in volume"""
  df_data_agg_pages = df_data_agg.groupBy("domain_code","page_title").agg(min("nb_views_by_day").alias("min_views_by_day"),
                                                                         max("nb_views_by_day").alias("max_views_by_day")).withColumn("max_delta",col("max_views_by_day")-col("min_views_by_day")).orderBy(desc("max_delta"))

  ### return the top n 
  return df_data_agg_pages.display(nb_top_pages)


#### CODE for Question 2
def get_pages_likely_the_most_impacted_by_an_event(path_to_load_data,country,nb_top_pages_with_high_variations,min_volumne =0):
  
  """ 
  I make the asumption that a page impacted by an event means it has a huge variation between the views of this page two following days. 
  There is a debate on the period to take to study the variation. it could have been hour(s) or days but I think one day is quite relevant
  I added a parameter of min volume if you don't want to be biased by huge variations but very small volume which can then be considered as outlier rather than relevant data   point
  The beginning of the function is the same as the previous one only end is changing
  """
  ###read file and load raw data frame
  df_data = spark.read.parquet(path_to_load_data)
  
  ##fill na
  df_data = df_data.na.fill(value = 0 , subset =["count_views"])
  
  ####select country on which you want 
  df_data= df_data.filter(col("domain_code").contains(f"{country}"))
  
  #### agg by day because by hour is too volatil ( I chosen day but it could also be week)
  df_data = df_data.withColumn('date', date_trunc(timestamp='date', format='day'))
  df_data_agg_by_day = df_data.groupBy("date","domain_code","page_title").agg(sum("count_views").alias("nb_views_by_day"))
  
  df_data_agg_by_day= df_data_agg_by_day.filter(col("nb_views_by_day")>=min_volumne)
  
  ###get the variation and absolute variation of page viewed with two consecutive days
  my_window = Window.partitionBy("domain_code","page_title",).orderBy("date")

  df_data_agg_by_day = df_data_agg_by_day.withColumn("previous_day", lag("date").over(my_window)).withColumn("nb_views_previous_day", lag("nb_views_by_day").over(my_window))
  df_with_variation = df_data_agg_by_day.withColumn("variation", when(isnull(col("nb_views_by_day") - col("nb_views_previous_day")), 0).otherwise(round(100*(col("nb_views_by_day") - col("nb_views_previous_day"))/col("nb_views_previous_day"),1)))
  df_with_variation = df_with_variation.withColumn("absolute_variation", abs(col("variation")))                              
                                    
  df_with_variation = df_with_variation.orderBy(desc("variation"))                                  

  ### return the top n 
  return df_with_variation.display(nb_top_pages_with_high_variations)

#### CODE for Question 3
def get_views_of_pages_on_a_subject(path_to_load_data,country,subject,min_volumne =0):
  
  """ 
  Two possibilities to answer question 3
  First way: we try to get the page title linked to the given subject ( Coupe du monde de footbal)
  Second way: you know that the wold cup happend between 2022-11-20 and 2022-12-18 so you take around that period and you are looking if the most subject searched thanks to   function for question 1 are related  to Footbal ( which is the case for these period cocnerning Footbal with Messi , Southgate , al ot of words linked to football)
  """
  ###read file and load raw data frame
  df_data = spark.read.parquet(path_to_load_data)
  
  ##fill na
  df_data = df_data.na.fill(value = 0 , subset =["count_views"])
  
  ####select country on which you want 
  df_data= df_data.filter(col("domain_code").contains(f"{country}"))
  
  ###select only pages on the given subject as parameter of the function after having cleaned the string of the page title
  df_data = df.withColumn("page_title_cleaned", regexp_replace(col("page_title"), "[:_]", ""))
  df_data = df_data.withColumn("page_title_cleaned",lower("page_title_cleaned"))
  df_data= df_data.filter(col("page_title_cleaned").contains(f"{subject}"))

  
  ##### agg by day because by hour is too volatil ( I chosen day but it could also be week)
  df_data = df_data.withColumn('date', date_trunc(timestamp='date', format='day'))
  df_data_agg_by_day = df_data.groupBy("date","page_title_cleaned").agg(sum("count_views").alias("nb_views_by_day"))
  df_data_agg_by_day.display()
  
  df_data_agg_by_day= df_data_agg_by_day.filter(col("nb_views_by_day")>=min_volumne)
  
  ###get the variation and absolute variation of page viewed on two following days
  my_window = Window.partitionBy("page_title_cleaned").orderBy("date")

  df_data_agg_by_day = df_data_agg_by_day.withColumn("previous_day", lag("date").over(my_window)).withColumn("nb_views_previous_day", lag("nb_views_by_day").over(my_window))
  df_with_variation = df_data_agg_by_day.withColumn("variation", when(isnull(col("nb_views_by_day") - col("nb_views_previous_day")), 0).otherwise(round(100*(col("nb_views_by_day") - col("nb_views_previous_day"))/col("nb_views_previous_day"),1)))
  df_with_variation = df_with_variation.withColumn("absolute_variation", abs(col("variation")))                              
                                    
  df_with_variation = df_with_variation.orderBy(desc("variation"))                                  


  #### return the views of the given subject
  return df_with_variation

#### CODE for Bonus question
def plot_evolution_of_views(path_to_load_data,country,page_title,by_hour = False):
  
  """ 
  The idea is the same of other function. 
  First we load the data, then we select the country 
  Then we clean the page title column by removing special characters and lower the string in order to filter the page title we want to plot evolution
  """
  ###read file and load raw data frame
  sdf_data = spark.read.parquet(path_to_load_data)
  
  ##fill na
  sdf_data = sdf_data.na.fill(value = 0 , subset =["count_views"])
  
  ###clean page title strung 
  sdf_data = sdf_data.withColumn("page_title_cleaned", regexp_replace(col("page_title"), "[:_]", ""))
  sdf_data = sdf_data.withColumn("page_title_cleaned",lower("page_title_cleaned"))
  
  ####select country and page title on which you want 
  sdf_data= sdf_data.filter(col("domain_code").contains(f"{country}")).filter(col("page_title_cleaned")== page_title)
  
  ##### agg by day because by hour is too volatil ( I chosen day but it could also be week)
  if by_hour == False:
    sdf_data = sdf_data.withColumn('date', date_trunc(timestamp='date', format='day'))
  
  sdf_data_agg_by_day = sdf_data.groupBy("date").agg(sum("count_views").alias("nb_views_by_day"))
  
  ###convert to Pandas
  df_data_agg_by_day = sdf_data_agg_by_day.toPandas()
  
  ##plot the dataframe
  df_data_agg_by_day.plot(x="date", y="nb_views_by_day", kind="scatter",title =f'Number of view by day for page title : {page_title}', figsize=(16, 8),fontsize=12)


