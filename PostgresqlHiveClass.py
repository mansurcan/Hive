from dataclasses import dataclass
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import Config

@dataclass()
class Data:
     df1: pd.DataFrame
     df2: pd.DataFrame

class Hive():
     """
     -------------------------Project Detils-------------------------
     - Create a csv document with 10000 employees HR details.
     - 8000 has common ID
     - 10% Null values
     """
     
     def __init__(self) -> None:
          """
          This is a constructor.
          It initialises the spark session.
          
          """
          self.spark = SparkSession.builder.config("spark.jars", "postgresql-42.5.0.jar").master("local").appName("PySpark_Postgres_test").getOrCreate()

     def read_df1(self):
          """
          It reads the hr_data from postgresql.
          """
     
          df1 = Config.DF1
          print(df1.show(5))

     def do_transformations(self, df1):
          """
          It does the transformations on hr_data.
           - Transformations: 
               - Reorder the coloumns
               - Rename the Null values
               - Increase the salary 10% on a new coloumn 
               - Concatinate first name and last name
          """
          
          # Combine first name and last name
          df=df1.na.fill({'First_Name':'No_First_Name', 'Email_Address':'No_Email_Address'})
          print(df.show(5))
          
          # Increase the salary %10 on a new coloumn
          df1["Salary"]=df1["Salary"].fillna(round(df1["Salary"].mean(skipna=True),2))
          df1["Salary_Increased"]=round(df1["Salary"]*1.1,2)

          # Rearrange NaN sick day values 
          df1["Sick_Days"]=df1["Sick_Days"].fillna(int(df1["Sick_Days"].mean(skipna=True)))
          
          # Add a phone number where there is a NaN value 
          df1["Phone_Number"]=df1["Phone_Number"].fillna("0203650981")

          print(df1.show(5))

          # Rename the dataframe
          df1 = df1[["ID","Full_Name","Job_Title","Salary","Salary_Increased",
                    "Sick_Days","Date_Time", "Street_Address", "City",
                    "Country","Phone_Number"]]

          print(df1.show(5))
     
     def read_df2(self):
          """
          It reads the hr_data_extra from postgresql.
          """

          df2 = Config.DF2
          print(df2.show(5))
          
     def merge_df1_df2(self, df1, df2):
          """
          It merges hr_data and hr_data_extra on common IDs.
          """
          df_merged = pd.merge(df1, df2, on='ID', how='inner')
          print(df_merged)

     def count_ids(self, df1, df2):
          """It is counting matching and non-matching IDs."""
          counting_ids = df1["ID"].isin(df2["ID"]).value_counts()
          print(counting_ids.head(5))

     def get_common_ids(self, df1, df2):
          """It gets common IDs."""
          same_num_ids = df1[df1["ID"].isin(df2["ID"])]
          print(same_num_ids.head(5))
     
     def get_non_common_ids(self, df1, df2):
          """It gets non common IDs."""
          not_same_ids = df1[np.where(df1["ID"].isin(df2["ID"]), False, True)]
          print(not_same_ids.head(5))


if __name__ == '__main__':
     hive = Hive()
     hive.read_df1()
     hive.read_df2()
     hive.merge_df1_df2()
     hive.do_transformations()
     hive.count_ids()
     hive.get_common_ids()
     hive.get_non_common_ids()