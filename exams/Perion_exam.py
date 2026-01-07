#----------------------------------------------------------
# import libraries
#----------------------------------------------------------
from pyspark.sql import SparkSession, dataframe, functions as F, DataFrame
import os
from pyspark import SparkConf

#----------------------------------------------------------
# main
#----------------------------------------------------------

# Q2:
# creaete spark session
spark = SparkSession.\
        builder.\
        appName('Perion_exam').\
        getOrCreate()

directory_path = "exam"
file_paths = [os.path.join(directory_path, file) for file in os.listdir(directory_path)]
combined_df = spark.read.json(file_paths[0])
dataframes = [spark.read.json(file) for file in file_paths]

for file_path in file_paths[1:]:
        df = spark.read.json(file_path)
        combined_df = combined_df.union(df)

#Q - 2.1
file_path = os.getcwd()
combined_df= combined_df.withColumn('df', F.to_date(F.col('dt').substr(1,10),'yyyy-MM-dd'))
combined_df.write.partitionBy('dt').option('path',file_path).saveAsTable('cpc_row_data',format='parquet', mode='overwrite')

#Q - 2.2
def write_to_destination(df:DataFrame, output_path:str, format:str, mode:str, table_name:str)-> bool:    
        try:
                df.write.partitionBy('dt').option('path',output_path).saveAsTable(table_name, format=format, mode=mode)
                return True
        except Exception as e:
                return False   

#Q - 2.3
result = write_to_destination(combined_df, file_path, 'parquet', mode='overwrite' , table_name='cpc_row_data') 
print(result)

current_working_directory = os.getcwd() + '/spark-warehouse/'
print(current_working_directory)

def set_one_file_per_partition(df:DataFrame, tbl_name:str)-> None:
        df = df.select('dt').distinct()
        partition_list = df.rdd.map(lambda row: row['dt']).collect()
        print(partition_list)
        for partName in partition_list:
                partition_df = df.filter(df['dt'] == partName)
                df_repart = partition_df.repartition(1)
                path = current_working_directory+f'/{tbl_name}/{partName}'
                df_repart.write.option('path',path).saveAsTable(tbl_name, format='parquet', mode='append')
                print(f'partition writen: {partName}')

set_one_file_per_partition(combined_df,'test')


# df = combined_df.groupBy(F.col('dt'),F.col('keyword')).agg( )

# print(result)
# #Q3: I woul use append to add data to data frame, and overwrite to delete al data at the source table.

#Q4: to increasse partitions 
        # to decrease you can use compaction and repartition 
        # to increase you can build a calculated column as needed and pertition by it, or set the required number of partition using 
        #repartition(n) when n is the number of requiered partitions.


