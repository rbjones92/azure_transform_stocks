## Azure - Transform Stocks From Blob Storage

## Summary
This project is an ETL of stock trade and quote data.  

## Description
The data is hosted on an Azure Blob container. It includes both CSV and JSON files. We will load both trade and quote CSV and JSON files, and combine them into one dataframe. 
We will then perform analytical ETL on the dataframe. We will find a 30 minute moving average for trades, and then insert the most recent trade price and moving average into quotes.

## Technologies
- Python 3.7
- Pyspark 2.4.4
- VS Code

## Local Execution
1. Load and combine Trades and Quotes into single dataframe
![Alt Text](screenshots/parquet_result.png?raw=true "parquet result")

2. Perform 30 minute moving average transformation
![Alt Text](screenshots/analytical_etl.png?raw=true "analytical etl result")
