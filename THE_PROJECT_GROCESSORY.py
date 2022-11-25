#!/usr/bin/env python3
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("MY_GROCESSORIES").getOrCreate()

df=spark.read.csv("statement.csv",header=True,inferSchema=True).fillna(0)

#change Date from string to_date, rename column names with special charaters
#Note: MM is capital not mm(minutes)
df=df.select(f.to_date(df.Date,'dd/MM/yy').alias("Date"),"Narration",\
             f.col("`Withdrawal Amt.`").alias("Withdrawal")\
            #,f.col("`Chq./Ref.No.`").alias("Ref_Number")\
            # ,f.col("`Deposit Amt.`").alias("Deposit")\
             #,"Closing Balance"\
            )

#Use Case : Removal of specific data
df = df.filter((df.Withdrawal != 30000) & (df.Withdrawal != 0.0))

    #Interesting that only &(and) and |(or) works here for removing two different entries

#Use Case: Grocessory (UPI and less then 2000 + that DMART transaction + Most(75% or 0.75) of the cash withdrawal)

df1 = df.withColumn("Grocessory",\
                f.when((f.col("Narration").startswith("UPI")) & (f.col("Withdrawal") < 2000.0),f.lit(1))\
                    .when(f.col("Narration").contains("DMART"),f.lit(1))\
                    .when(f.col("Narration").rlike("^(UPI-|EAW-|NWD-).*"),f.lit(1))\
                    .otherwise(f.lit(0))
               )
df1 = df1.withColumn("Spent",\
                      f.when(f.col("Narration").rlike("^(EAW-|NWD-).*"),f.col("Withdrawal")*0.75)\
                     .otherwise(f.col("Withdrawal"))
                    )

#df1.show(1000)

grocesseries = df1.select("*",f.date_format(f.col("Date"),"yyyy-MM").alias("year_month"))\
                    .where(f.col("Grocessory") == 1).drop("Date","Grocessory","Narration","Withdrawal")\
                        .groupBy("year_month").sum()

grocesseries = grocesseries.select(f.col("year_month"),f.round(f.col("sum(Spent)")).alias("Spent"))\
                .sort("year_month")


#convert pyspark DF to PandasDF(pdf) and then plot
pdf = grocesseries.toPandas()

import plotly                             #### You need to have pandas installed
import plotly.express as px               #to create interactive charts
import plotly.graph_objects as go

px.bar(pdf,x="year_month",y=pdf["Spent"],title="Grocessory Spending").show()
