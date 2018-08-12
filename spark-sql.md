# Spark SQL

## DataFrame
[DataFrame](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.DataFrame) 

Lectura de un csv
	val skills = spark.read.option("header", "true")
							.option("inferSchema", "true")
							.option("delimiter", ";")
							.csv("/arturo/skills.csv")
							
val mydata = List(("Nombre","Apellidos"),("Enrique","Pérez"),("Jose","López"),("Harry","Potter"))

val myDF = spark.createDataFrame(mydata)							