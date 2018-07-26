package intro.sql

import org.apache.spark.sql.SparkSession


/**
 * You can query using SQL in addition to DataFrame and Dataset operations
─ Incorporates SQL queries into procedural development
▪ You can use SQL with Hive tables and temporary views
─ Temporary views let you use SQL on data in DataFrames and Datasets
▪ SQL queries and DataFrame/Dataset queries are equivalent
─ Both are optimized by Catalyst
▪ The Catalog API lets you list and describe tables, views, and columns, choose a
database, or delete a view
 */
object Accounts extends App {
  
    
  val spark = SparkSession.builder.getOrCreate()
  
      spark.catalog.listTables.show
    
    spark.catalog.listColumns("accounts").show
    
    val accountsDF = spark.read.table("accounts")
    accountsDF.printSchema
    
    val firstLastDF = spark.sql("SELECT first_name,last_name FROM accounts")
    firstLastDF.printSchema
    firstLastDF.show(5)
    
    val firstLastDF2 = accountsDF.select("first_name","last_name")
    firstLastDF2.printSchema
    firstLastDF2.show(5)
    
    val accountDeviceDF = spark.read.option("header","true").option("inferSchema","true").csv("/loudacre/accountdevice")
    
    accountDeviceDF.createOrReplaceTempView("account_dev")
    
    spark.catalog.listTables.show
    
    spark.sql("SELECT * FROM account_dev LIMIT 5").show
    
    val nameDevDF = spark.sql("SELECT acct_num, first_name, last_name, account_device_id FROM accounts JOIN account_dev ON acct_num = account_id")
    nameDevDF.show
    
    nameDevDF.write.option("path","/loudacre/name_dev").saveAsTable("name_dev")
    
    spark.catalog.listTables.show
    spark.sql("DESCRIBE name_dev").show
  
    spark.stop
  
  
}