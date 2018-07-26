package intro.rdd.pair


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * PAIR-RDD
 */
object WebLogs extends App {
    
  
   val spark = SparkSession.builder.getOrCreate()
  
   val sc = spark.sparkContext
  
   // 1) Explore Web Log Files
    val wl2RDD = sc.textFile("/loudacre/weblogs/*2.log")

    //Using map-reduce logic, count the number of requests from each user.
    val wl2RDD1 = wl2RDD.map(_.split(" ")).map(x=>(x(2),1)).reduceByKey(_+_)
    
    //Use countByKey to determine how many users visited the site for each frequency.
    //That is, how many users visited once, twice, three times, and so on.
    //val freqHitMap = wl2RDD1.map{case (user, hit) => (hit, user)}.countByKey()
    val freqHitMap = wl2RDD1.map(_.swap).countByKey()


    //Create an RDD where the user ID is the key, and the value is the list of all the IP
    //addresses that user has connected from. (IP address is the first field in each request
    //line.)
    val userIPs = wl2RDD.map(_.split(" ")).map(x=>(x(2),x(0))).groupByKey

    //print
    for (userIP <- userIPs.take(10)) {
       println(userIP._1 + ":")
       for (ip <- userIP._2) println("\t"+ip)
    }

    //2) Join Web Log Data with Account Data
    val aRDD = sc.textFile("/user/hive/warehouse/accounts")
    
    //val accRDD = aRDD.map(_.split(",")).map(acc=>(acc(0),acc))
    val accRDD = aRDD.map(_.split(",")).keyBy(_(0))
    
    //Joining by Keys
    val accwl2RDD = accRDD.join(wl2RDD1)
    
    //Display the user ID, hit count, and first name (4th value) and last name (5th
    //value) for the first five elements. The output should look similar to this:
    accwl2RDD.map{case ( id, rowHit) => (id, rowHit._2, rowHit._1(3)+" " + rowHit._1(4))}
  
    
    //Use keyBy to create an RDD of account data with the postal code (9th field in theCSV file) as the key.
    val pcRDD = aRDD.map(_.split(",")).keyBy(x=>x(8)).map{case (pc,row) => (pc, row(3) + "," + row(4))}
    
    //Create a pair RDD with postal code as the key and a list of names (Last Name,First
    //Name) in that postal code as the value.
    //val pcNames = pcRDD.map{case (pc,row) => (pc, row(3) + "," + row(4))}.groupByKey
    val pcNames = pcRDD.mapValues(row => row(4) + "," + row(3)).groupByKey
    
    //Sort the data by postal code
    pcNames.sortByKey(false)
    
    spark.stop()
  
}