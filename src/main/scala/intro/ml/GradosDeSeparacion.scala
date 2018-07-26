import org.apache.spark.SparkContext
import collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

object GradosDeSeparacion
{
  def main(args: Array[String])
  {
    if (args.length < 2) {
      System.err.println("Uso: GradosDeSeparacion fichero vertice")
      System.exit(1)
    }
    val fichero = args(0)
    val verticeOrigen = args(1).toInt

    val sc = new SparkContext()
    sc.setLogLevel( "ERROR" )

    type ValueRegistro = ( ArrayBuffer[Int], Int, String) 
    type Registro = ( Int, ValueRegistro )

    def convertirBFS( line : String ) = {
      val campos = line.split( ' ' )

      val vertice = campos(0).toInt

      val conexiones = campos.tail.map(_.toInt).
        foldLeft( ArrayBuffer[Int]() )( _ += _ )

      val (color, distancia) =
        if (vertice == verticeOrigen) ("GRIS", 0) else ("BLANCO", 9999)

      (vertice, (conexiones, distancia, color))
    }


    def crearRDDInicial( fichero : String ) =
      sc.textFile( fichero ).map( convertirBFS )

    def bfsMap( node : Registro ) = {
      val (vertice, (conexiones, distancia, color) ) = node

      if( color != "GRIS" )
        List(node)
      else {
        def nuevosRegistros = 
          conexiones.foldLeft( List[Registro]() ) (
          _ :+ ( _, (ArrayBuffer[Int](), distancia+1, "GRIS" ) ) )

        nuevosRegistros :+ (vertice, (conexiones, distancia, "NEGRO") )
      }
    }


    def bfsReduce( data1 : ValueRegistro, data2 : ValueRegistro ) =
    {
      val( vertices1, distancia1, color1 ) = data1
      val( vertices2, distancia2, color2 ) = data2

      val vertices = vertices1 ++ vertices2

      val distancia = math.min( distancia1, distancia2 )

      val color = 
        if (color1 == "NEGRO" || color2 == "NEGRO")
          "NEGRO"
        else if (color1 == "GRIS" || color2 == "GRIS")
          "GRIS"
        else
          "BLANCO"

      (vertices, distancia, color)
    }


    def hayGrises( iterationRDD : RDD[Registro] ) =
    {
      def esGris( r : Registro ) =
      {
        val (vertice, (conexiones, distancia, color) ) = r
        color == "GRIS"
      }

      val grises = iterationRDD.filter( esGris( _ ) )
      grises.count != 0
    }

    var iterationRDD = crearRDDInicial( fichero )

    // si verticeOrigen está en el RDD
    if( iterationRDD.lookup( verticeOrigen ).length != 0 ) {
      while ( hayGrises( iterationRDD ) ) {
        val mapeo = iterationRDD.flatMap( bfsMap )
        iterationRDD = mapeo.reduceByKey( bfsReduce )
      }

      /*
       * Imprimir distancias
       */
      // Los pares son la distancia y el vértice que está a esa distancia del origen
      val verticeDistancia = iterationRDD.
        map{ case( vertice, ( vertices, distancia,  color) )  => ( vertice, distancia ) }

      import scala.collection.immutable.SortedMap
      val mapVD = SortedMap( verticeDistancia.collect:_* )

      println( )
      println( "vértice distancia" )
      for( ( vertice, distancia) <- mapVD )
        println( s"  ${vertice}\t${distancia}" )

      /*
       * Imprimir resumen
       */
      // Los pares son la distancia y el vértice que está a esa distancia del origen
      val distanciaNroVertices = iterationRDD.
        map{ case( vertice, ( vertices, distancia,  color) )  => ( distancia, vertice ) }

      import scala.collection.immutable.SortedMap
      val map = SortedMap( distanciaNroVertices.countByKey.toArray:_* ).tail

      println( )
      println( "dist nro_elems" )
      for( ( dist, num) <- map )
        println( s"  ${dist}\t${num}" )
    }
    else
      println( s"El vértice ${verticeOrigen} no existe")
  }
}

