package intro.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.MapValues

object WordLegth extends App {
  
    if (args.length < 1) {
      System.err.println("Usage: solution.WordLegth <file>")
      System.exit(1)
    }
 
    val wordsFile = args(0)
    
    val spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val wordsRDD = spark.sparkContext.textFile(wordsFile).flatMap(line => line.split(" "))
      .map(word => (word(0), word.length()))
      .groupByKey
      //.mapValues( _. )
      //.MapValues(v => sum(v)/v.legth())

    spark.stop()
 
}