package intro.sql.df


/**
 * Crea un DataFrame a partir de una tabla llamada “clientes” almacenada en una base de datos MySQL
 
val url = "jdbc:mysql://ip:port/test"
val df = sqlContext
       .read
       .format("jdbc")
       .option("url", url)
       .option("dbtable", "clientes")
       .load()
 
// Muestra el esquema del DataFrame&lt;
df.printSchema()
 
// Cuenta a los clientes por su edad
val countsByAge = df.groupBy("edad").count()
countsByAge.show()
 */
object BDatos {
  
}