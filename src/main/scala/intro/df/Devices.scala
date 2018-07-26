package intro.df

import org.apache.spark.sql.SparkSession

/**
 * ejercicio pag 53 Join Account Data with Cellular Towers by Zip Code
 * 
 * Join de varios DFs
 * 
 * En los DataFrames
 * Los esquema se determinan de forma EAGER 
 * Las TRANSFORMACIONES son LAZY solo se ejecutan cuando una ACCIÓN la invoca 
 * 
 * Transformaciones son:
 * FILTRADO: val sorrento10 = devDF.where("make='Sorrento'").where("model like 'F3%'")
 * 
 * Acciones sobre un dataframe:
 * devDF.show(10,false) //muestra 10 rows sin truncar
 * val rows = devDF.take(5) //toma 5 rows y se lo asigna a rows
 * scala> rows.foreach(println)	
 * 
 * Herramientas parqueta para manejar archivos parquet
 * parquet-tools head hdfs://localhost/loudacre/top_devices
 * 
 * OJO la comparacion de campos en los join es 
 * === igualdad
 * =!= desigualdad
 */
object Devices extends App {
  
  
      val spark = SparkSession.builder.getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      
      
      //DF de cuentas
      val accountsDF = spark.read.table("accounts")
    
      //DF de dispositivos
      val devicesDF = spark.read.json("/loudacre/devices.json")					 
    
      //DF de cuentas-dispositivos
      val accountdeviceDF = spark.read.format("csv").option("inferSchema", "true")
                          .option("header", "true")
                          .load("/loudacre/accountdevice/")
    
    
     val join = accountsDF.join(accountdeviceDF, accountsDF("acct_num") === accountdeviceDF("account_id"))
    					 .join(devicesDF, accountdeviceDF("device_id") === devicesDF("devnum"))
    					 .where(accountsDF("acct_close_dt").isNull )
    					 .groupBy("device_id", "make", "model")
    					 .count()
    					 .withColumnRenamed("count","active_num")
    					 
    join.orderBy(join("active_num").desc)
    
    join.write.mode("overwrite").save("/loudacre/top_devices")

    val activeAccountsDF = spark.read.table("accounts")
    
    activeAccountsDF.where(activeAccountsDF("acct_close_dt").isNull )

    val activeDevicesIDsDF = activeAccountsDF.join(accountdeviceDF, activeAccountsDF("acct_num") === accountdeviceDF("account_id")).select("device_id")

    val activeDevices = activeDevicesIDsDF.join(devicesDF, activeDevicesIDsDF("device_id") === devicesDF("devnum")).groupBy("device_id", "make", "model")
  
    spark.stop()

}