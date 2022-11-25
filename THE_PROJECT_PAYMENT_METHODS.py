#!/usr/bin/env python3
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("MY_PAYMENT_METHODS").getOrCreate()

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

#Use Case: UPI vs POS VS ATW

#First 3 letters of narration are Transaction types, add no of transactions(1) for each row
df1 = df.select("*",f.date_format(f.col("Date"),'yyyy-MM').alias("year_month")\
                     ,f.col("Narration").substr(1,3).alias("Transaction_Type")\
                        ,f.lit(1).alias("transaction_number")\
               )\
        .drop("Narration","Date")

#df1.show(1000)

df2 = df1.withColumn("Transaction_type",\
               f.when(f.col("Transaction_Type").contains("ATW"),\
                      f.regexp_replace(f.col("Transaction_Type"),"ATW","NWD"))\
               .when(f.col("Transaction_Type").contains("EAW"),\
                      f.regexp_replace(f.col("Transaction_Type"),"EAW","NWD"))\
                .otherwise(f.col("Transaction_Type"))
              )

#get unique transaction types
#df1.select("Transaction_type").distinct().show()  #EAW,NWD,ATW(ATM)-INS(bank alerts)-IB-POS-UPI

#Group no of transaction and amount
transaction = df2.groupBy([f.col("year_month"),f.col("Transaction_type")]).sum()
transaction = transaction.select("*",f.round(f.col("sum(Withdrawal)")).alias("Spent_money")\
                                    ,f.col("sum(transaction_number)").alias("No_of_transactions")\
                                ).drop(f.col("sum(transaction_number)"))\
                                 .drop(f.col("sum(Withdrawal)"))\
                            .sort("year_month")
#transaction.dtypes

#convert pyspark DF to PandasDF(pdf) and then plot
pdf = transaction.toPandas()

import plotly                             #### You need to have pandas installed
import plotly.express as px               #to create interactive charts
import plotly.graph_objects as go

px.bar(pdf,x="year_month",y=pdf["No_of_transactions"],barmode="group",color="Transaction_type",\
       title="No of Transaction for different mode of Payments").show()

px.bar(pdf,x="year_month",y=pdf["Spent_money"],barmode="group",color="Transaction_type",\
      title="Amount spent on different mode of Payments").show()
