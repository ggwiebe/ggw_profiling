# Databricks notebook source
# MAGIC %md ## Profile Data

# COMMAND ----------

# MAGIC %md ### 1) Dataframe display() Data Profile add-on

# COMMAND ----------

# my_df = spark.table('ggw.ggw_covid.covid_hospitalizations_country')
my_df = spark.table('ggw_mloc.ggw_babs.trip') # Profile run in 6.23 seconds

display(my_df)

# COMMAND ----------

# MAGIC %md ### 3) bamboolib - Data Profiling  
# MAGIC   
# MAGIC Create a pandas dataframe Bamboolib will then launch. 
# MAGIC - The Data tab will show a grid of the data
# MAGIC - Click "Explore" to get a picture of all the columns
# MAGIC - Select a particular column to get a more complete profile of that column

# COMMAND ----------

import bamboolib as bam

# trip_df = spark.sql('SELECT * FROM ggw_mloc.ggw_babs.trip')
trip_df = spark.table("ggw_mloc.ggw_babs.trip").toPandas()

trip_df

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from utils.sparkprofiling import dataprofile

cols_toProfile = my_df.columns
my_df_profile = dataprofile(my_df,cols_toProfile)

display(my_df_profile)

# COMMAND ----------

print(my_df_profile)

# COMMAND ----------

display(my_df_profile)

# COMMAND ----------


