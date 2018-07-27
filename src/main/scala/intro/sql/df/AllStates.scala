package intro.sql.df

import org.apache.spark.sql.SparkSession

/**
 * 
 */
object AllStates extends App {
  
  val spark = SparkSession.builder.getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val accountsDF = spark.read.table("accounts")
  
  accountsDF.select("state").distinct().collect().foreach { stateCode => extractState(stateCode.get(0).toString())}
  
  def extractState(stateCode: String) { 
    val stateAccountsDF = accountsDF.where(accountsDF("state") === stateCode)
    stateAccountsDF.write.mode("overwrite").save("/loudacre/accounts_by_state/"+stateCode)
  }

  spark.stop()
  
}