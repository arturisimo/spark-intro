# Introducción a Spark


## intro
**Apache Spark** es un motor para el procesamiento de grandes cantidades de datos. Está escrito en Scala, ofrece un desempeño rápido ya que el almacenamiento de datos se gestiona en memoria.

La industria venia utilizando **Apache Hadoop** desde 2006 para almacenar, procesar y analizar grandes volúmenes de datos. Hadoop se basa en el modelo de programación MapReduce y procesa los datos en disco.
Spark fue un subproyecto de Hadoop desarrollado en 2009 por Matei Zaharia en la Universidad de Berkeley AMPLab. En 2013, el proyecto Spark fue donada a Apache.

Spark no posee un sistema de archivos utiliza el sistema de archivos de Hadoop (HDFS)

Spark provee un stack de libraries construidas sobre el core  
	* **Spark Core** provee los **Resilient Distributed Datasets (RDDs)**
	* **Spark SQL** trabaja con datos estructurados (con Schema): **DataFrame** y **Dataset**
	* **MLlib** framework para el desarrollo de Machine learning
	* **Spark Streaming** procesamiento de datos en tiempo real
	* **GraphX** entorno de procesamiento gráfico, proporciona una API para gráficos y cálculo gráfico en paralelo.

Las aplicaciones de Spark son ejecutadas independientementes y estas son coordinadas por el objeto **SparkContext** del **Driver Program**. SparkContext es capaz de conectarse al **Cluster Manager** que se encargan de asignar recursos en el sistema. Una vez conectados, Spark puede encargar que se creen **executors**
encargados de ejecutar tareas (**tasks**) en los nodos del clúster.

 	* Task - unidad de trabajo que se ejecuta
 	* Stage - Conjunto de tareas asociados a un job que se pueden ejecutar en paralelo
 	* Jobs - Secuncia de stages como resultado de una accion
 	* Application conjunto de jobs manejador por un único driver
 	
 	
## Spark Core RDD 	
Los **RDD Resilient Distributed Dataset** [org.apache.spark.rdd.RDD] representa una coleccion distribuida de elementos, de cualquier tipo, y sin estructurar no tienen esquema asociado.
Estos RDDs permiten cargar gran cantidad de datos en memoria para un veloz procesamiento y además pueden dividirse en **particiones** para ser tratado de forma paralela.

Los RDDs se generan a partir de datos:
	* Datos en memoria
	* Ficheros planos 
	* Otros RDD Data in other RDDs
	* DataFrames y Datasets

Operaciones RDD 
 	* Transformaciones generar un RDD a partir de otro. Se clasificacn en 
 		- **Narrow** los datos a tratar están ubicados en la misma partición del RDD: filter(), sample(), map(), flatMap()...
		- **Wide** se mezclan datos de varias particiones: groupByKey() o reduceByKey()
 	* Actiones devuelve un valor a la aplicación partir del RDDs
 
 Una query RDD consiste en una secuencia de transformaciones ejecutadas por una accion, esto es, se ejecutan de forma **lazy** cuando las invoca una acción

## Spark SQL: DF y DS
Spark SQL se utiliza datos estructurados mediante **DataFrames** [org.apache.spark.sql.DataFrame]. Los DF internamente generan un esquema, conceptualmente son equivalentes a las tablas de una BBDD relacional. 

 La API de Spark SQL permite la conexión a las fuentes de origen, obteniendo así los datos y pasando a ser gestionados en memoria mediante Spark.

Son colecciones inmutable de objetos de tipo **Row** (parecido a una tupla).

Los esquema se determinan de forma **eager**, al igual que los RDD las transformaciones son lazy solo se ejecutan cuando una acción la invoca 

DataFrames pueden ser construidos a partir de diversas fuentes de datos:
  ─ ficheros planos (txt,csv,json): Datos semi-estrucurados su estructura se puede inferir (**inferSchema**)
  ─ Binarios : Apache Parquet ─ Apache ORC - Datos Estructurados
  ─ Tablas: Hive metastore ─ JDBC - Cassandra
  - RDD existente
  
Se referencian con URI absolutas o relativas:
   myfile.json
   hdfs://nnhost/loudacre/myfile.json
   file:/home/training/myfile.json


Los **Dataset** es una coleccion distribuida de objetos fuertemente tipados suelen estar formados por Rows de  Clases case. Así pues, es un tipo específico de DataFrames