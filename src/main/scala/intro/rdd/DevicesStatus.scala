package intro.rdd

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object DevicesStatus extends App {
    
   val spark = SparkSession.builder.getOrCreate()
  
   val sc = spark.sparkContext
  
    //Upload the devicestatus.txt file to HDFS.
    val dsRDD = sc.textFile("/loudacre/devicestatus.txt")

//    18. Determine which delimiter to use (the 20th character—position 19—is the first use of the delimiter).
//    19. Filter out any records which do not parse correctly (hint: each record should have exactly 14 values).
    val fDsDD = dsRDD.filter(line => line.split(line(19)).length == 14)

//    20. Extract the date (first field), model (second field), device ID (third field), and latitude and longitude (13th and 14th fields respectively).
//    21. The second field contains the device manufacturer and model name (such as Ronin
//    S2). Split this field by spaces to separate the manufacturer from the model (for
//    example, manufacturer Ronin, model S2). Keep just the manufacturer name.
    val csvDsRDD = fDsDD.map(line => line.split(line(19))).map(v => v(0)+','+v(1).split(' ')(0)+','+v(2)+','+v(12)+','+v(13))

    //22. Save the extracted data to comma-delimited text files in the
    csvDsRDD.saveAsTextFile("/loudacre/devicestatus_etl")

  //== ALTERNATIVA convertirlo a dataFrame
  val rowsDsRDD = fDsDD.map(line => line.split(line(19))).map(v => Row(v(0),v(1).split(' ')(0),v(2),v(12),v(13)))

  //generacion estructura
  val mySchema = StructType(Array(StructField("date", StringType),StructField("model", StringType),StructField("device ID", StringType),StructField("latitude", StringType), StructField("longitude", StringType)))

  //los DF tienen siempre una estructura los RDD no 
  val dsDF = spark.createDataFrame(rowsDsRDD,mySchema)
  
  spark.stop()
}