import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col

def dataprofile(data_all_df,data_cols):

  print("Profile starting for columns {}...".format(data_cols))

  # ====================================================================================================
  # Select only the columns you want to profile
  # ====================================================================================================
  data_df = data_all_df.select(data_cols)
  columns2Bprofiled = data_df.columns

  # ====================================================================================================
  # Get global variables for schema/database and table names
  # ====================================================================================================
  global schema_name, table_name
  if not 'schema_name' in globals():
      schema_name = 'schema_name'
  if not 'table_name' in globals():
      table_name = 'table_name' 

  # ====================================================================================================
  # Create a data profiling dataframe to track the tables and the columns within it to profile
  # ====================================================================================================
  dprof_df = pd.DataFrame({'schema_name':[schema_name] * len(data_df.columns),\
                             'table_name':[table_name] * len(data_df.columns),\
                             'column_names':data_df.columns,\
                            #  'col_names':data_df.columns,\
                             'data_types':[x[1] for x in data_df.dtypes]}) 
  dprof_df = dprof_df[['schema_name','table_name','column_names', 'data_types']]
  dprof_df.set_index('column_names', inplace=True, drop=True)
  # print("Data Profiling dataframe created: {}".format(dprof_df))

  # ====================================================================================================
  # Profile: number of rows
  # ====================================================================================================
  print("Profile starting row counts...")
  num_rows = data_df.count()
  dprof_df['num_rows'] = num_rows
  # print("Profile so far: {}".format(dprof_df))

  # ====================================================================================================
  # Profile: number of rows with nulls and nans   
  # ====================================================================================================
  print("Profile starting row null & nans counts...")
  df_nacounts = data_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data_df.columns \
                                if data_df.select(c).dtypes[0][1]!='timestamp']).toPandas().transpose()
  df_nacounts = df_nacounts.reset_index()  
  df_nacounts.columns = ['column_names','num_null']
  df_nacounts.set_index('column_names', inplace=True, drop=True)
  # print(df_nacounts)
  dprof_df = pd.merge(dprof_df, df_nacounts, on = 'column_names', how = 'left')
  # print("Profile so far: {}".format(dprof_df))

  # ====================================================================================================
  # Profile: number of rows with white spaces (one or more space) or blanks
  # ====================================================================================================
  print("Profile starting white space row counts...")
  num_spaces = [data_df.where(F.col(c).rlike('^\\s+$')).count() for c in data_df.columns]
  dprof_df['num_spaces'] = num_spaces
  num_blank = [data_df.where(F.col(c)=='').count() for c in data_df.columns]
  dprof_df['num_blank'] = num_blank
  # print("Profile so far: {}".format(dprof_df))

  # ====================================================================================================
  # Profile: using the in built describe() function 
  # ====================================================================================================
  print("Profile starting built-in describe function...")
  desc_df = data_df.describe().toPandas().transpose()
  desc_df.columns = ['count', 'mean', 'stddev', 'min', 'max']
  desc_df = desc_df.iloc[1:,:]  
  desc_df = desc_df.reset_index()  
  desc_df.columns.values[0] = 'column_names'  
  desc_df = desc_df[['column_names','count', 'mean', 'stddev']]
  desc_df.set_index('column_names') 
  dprof_df = pd.merge(dprof_df, desc_df , on = ['column_names'], how = 'left')
  # print("Profile so far: {}".format(dprof_df))

  # ====================================================================================================
  # Profile: min/max values & counts
  # ====================================================================================================
  print("Profile starting min and max counts...")
  allminvalues = [data_df.select(F.min(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
  allmaxvalues = [data_df.select(F.max(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
  print(f"Type is :{type(allminvalues)}")
  print(f"allmaxvalues: {allmaxvalues}")
  print("\nThe list(zip) is: {}".format(list(zip(columns2Bprofiled, allmaxvalues))))
  print(f"allminvalues: {allminvalues}")
  print("\nThe zip _ amm is: {}".format(zip(columns2Bprofiled, allminvalues)))
  print("\nThe list(zip) is: {}".format(list(zip(columns2Bprofiled, allminvalues))))
  print(f"Count for id: {data_df.where(F.col('id') == F.lit(913460)).count()}")
  print(f"Count for start_date: {data_df.where(F.col('start_date') == F.lit('9/9/2014 9:59')).count()}")
  # allmincounts = [data_df.where(F.col(x).cast("string") == F.lit(y).cast("string")).count() for x,y in list(zip(columns2Bprofiled, allminvalues))]

  for x, y in list(zip(columns2Bprofiled[1:], allminvalues[1:])):
    print(f"x: {x}, y: {y}")
    print(f"""Running: data_df.where(F.col(f"'{x}'") == F.lit({y})).count()""")
    cnt = data_df.where(F.col(f"'{x}'") == F.lit(y))#.count()
    cnt.show(10)
    # print(f"""Output: {cnt}""")
  
  asssert(False)
  # allmaxcounts = [data_df.where(F.col(x) == y).count() for x,y in list(zip(columns2Bprofiled, allmaxvalues))]
  
  print('The lists are:', '\n'.join([str("x={}; y={}".format(x,y)) for x,y in list(zip(columns2Bprofiled, allminvalues))]))
  display(list(zip(columns2Bprofiled, allminvalues)))
  # print('allmincounts:')
  # display(allmincounts)
  # print('allmaxcounts:')
  # display(allmaxcounts)
  # return dprof_df

  df_counts = dprof_df[['column_names']]
  df_counts.insert(loc=0, column='min', value=allminvalues)
  # df_counts.insert(loc=0, column='counts_min', value=allmincounts)
  df_counts.insert(loc=0, column='max', value=allmaxvalues)
  # df_counts.insert(loc=0, column='counts_max', value=allmaxcounts)

  # df_counts = df_counts[['column_names','min','counts_min','max','counts_max']]
  df_counts = df_counts[['column_names','min','max']]
  dprof_df = pd.merge(dprof_df, df_counts , on = ['column_names'], how = 'left')  
  print("Profile so far: {}".format(dprof_df))

  # ====================================================================================================
  # Profile: number of distinct values in each column
  # ====================================================================================================
  print("Profile starting distinct value counts...")
  dprof_df['num_distinct'] = [data_df.select(x).distinct().count() for x in columns2Bprofiled]
  # print("Profile so far: {}".format(dprof_df))

  # ====================================================================================================
  # Profile: most/least frequently occuring value in a column and its count
  # ====================================================================================================
  print("Profile starting least and most frequent counts...")
  dprof_df['most_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=False).limit(1).\
                                     toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
  dprof_df['most_freq_value'] = [x[0] for x in dprof_df['most_freq_valwcount']]
  dprof_df['most_freq_value_count'] = [x[1] for x in dprof_df['most_freq_valwcount']]
  dprof_df = dprof_df.drop(['most_freq_valwcount'],axis=1)

  dprof_df['least_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=True).limit(1).\
                                      toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
  dprof_df['least_freq_value'] = [x[0] for x in dprof_df['least_freq_valwcount']]
  dprof_df['least_freq_value_count'] = [x[1] for x in dprof_df['least_freq_valwcount']]
  dprof_df = dprof_df.drop(['least_freq_valwcount'],axis=1)

  print("Profile completed: {}".format(dprof_df))

  return dprof_df