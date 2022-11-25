#!/usr/bin/env python3
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("MY_SPENDINGS").getOrCreate()

df=spark.read.csv("statement.csv",header=True,inferSchema=True).fillna(0)

#change Date from string to_date, rename column names with special charaters
#Note: MM is capital not mm(minutes)
df=df.select(f.to_date(df.Date,'dd/MM/yy').alias("Date"),"Narration",\
             f.col("`Withdrawal Amt.`").alias("Withdrawal"),\
            #f.col("`Chq./Ref.No.`").alias("Ref_Number"),\
             f.col("`Deposit Amt.`").alias("Deposit")\
             #,"Closing Balance"\
            )

#Use Case : Removal of specific data
df = df.filter((df.Deposit != 30000) & (df.Withdrawal != 30000) & (df.Deposit != 144807))

#df.show(1)
#Remove Interest if needed

    #Interesting that only &(and) and |(or) works here for removing two different ... \
    #... entires, where one have 30000 in Withdrawal and a different one have 30000 in Deposit

import calendar

#Use Case : Monthly saving vs spending
#Logic : Salary gets credited on the last working day, so we need to add last working day to 
        #... next month and then groupby and sum the Income and Expenditure

#calendar.monthcalendar takes year and month as input. [-1] to reverse month lists and [0] pick last one

last_day = f.udf(lambda x,y: max(calendar.monthcalendar(int(x),int(y))[-1:][0][:5]))

df = df.withColumn("Last_Workingday",last_day((f.date_format(df.Date,'yyyy')),\
                                                 (f.date_format(df.Date,'MM'))))

#Or you can use split to get the same output
#dfs = dfs.withColumn("Last_Workingday",last_day((f.split(f.col("Date"),"-")[0]),\
#                                                (f.split(f.col("Date"),"-")[1])))


#Adding column that shall group based on the last working day
#sometimes lastworking day could be holiday, so we need to check the "SALARY" credited date
df = df.withColumn("year_month",\
                 f.when((f.date_format(df.Date,'dd') < df.Last_Workingday) & \
                        (~df.Narration.contains("SALARY")) ,f.date_format(df.Date,"yyyy-MM")).\
                 otherwise( f.date_format(f.date_add(df.Date,5),"yyyy-MM") ))

#Spending
expenditure = df.select("Deposit",f.round(f.col("Withdrawal")).alias("Withdrawal"),\
                        "year_month").\
                    groupBy("year_month").sum()

#Savings
savings = expenditure.withColumn("savings",f.round(f.col("sum(Deposit)")-f.col("sum(Withdrawal)"))).\
                                                                                    sort("year_month")
savings = savings.select("year_month",f.col("sum(Deposit)").alias("Income"),\
                         f.col("sum(Withdrawal)").alias("Expenditure"),"savings")

sum_array = savings.groupBy().sum().select("*",f.col("sum(savings)").alias("Total_Savings"))\
                                    .drop("sum(savings)")
avg_array = savings.groupBy().avg().select("*",f.round(f.col("avg(Expenditure)")).alias("Avg Monthly Expenditure"))\
                                    .drop("avg(Expenditure)")

for item in avg_array.collect():    #collect gives you row wise output and each row converts to dictonary
    avg_dict = item.asDict()
    
for item in sum_array.collect():
    sum_dict = item.asDict()


#convert pyspark DF to PandasDF(pdf) and then plot
pdf = savings.toPandas()

import plotly                             #### You need to have pandas installed
import plotly.express as px               #to create interactive charts
import plotly.graph_objects as go

print(f"You saved {sum_dict['Total_Savings']} Rupees, till now")
print(f"You are spending an average of {avg_dict['Avg Monthly Expenditure']} Rupees per month")

px.bar(pdf,x="year_month",y=[pdf["Income"],pdf["Expenditure"],pdf["savings"]],\
                        barmode="group",title="Income vs Expenditure vs Savings").show()


#Learnings:
    #fs = df1.filter(df1.Narration.contains("SALARY"))
    #f.udf  #have to regularly check 
