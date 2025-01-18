
Each day a large file is loaded to our SQL database containing all customer orders, the items in that order and customer name.

## Part 1

This is becoming difficult to manage, so please devise a process to normalise the data coming in, such as by using a star schema.

This can be done however you feel is appropriate, use any libraries you feel are needed and submit the code and the final application.db file.

## Part 2

Now the data has been transformed to make it easier to store, some of the reports need to be updated. Please provide SQL queries that can provide:

1) The total number of orders for each customer
2) The most recent order for each customer
3) For the past week, which customers have provided us with the most value

## Part 3

We currently manually upload this file from our supplier into S3 and run a copy command to get this table into redshift. But, they have a REST API we can get the same data from. How could this process be automated/improved using cloud services?


## BONUS

Build an API that can generate fake data for the raw_orders table. Build an ETL pipeline using Databricks to get the normalised data into a Databricks SQL warehouse. Create a dashboard using Databricks SQL dashboards to visualise the 3 metrics (Part 2).