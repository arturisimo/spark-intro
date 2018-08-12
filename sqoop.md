# Sqoop: Ingesta de datos

Apache Sqoop (SQL-to-Hadoop) es una herramienta de línea de comando que se utiliza para transferir grandes volúmenes de datos de bases de datos relacionares a un cluster Hadoop (y viceversa)

Concretamente transforma datos relacionarles en Hive o HBase en una dirección y en la otra de HDFS a datos relacionares como MySQL, Oracle, Postgres o a un data warehouse.

## MySQL
	
	$ mysql -u root

	mysql> show databases

	mysql> use loudacre

	mysql> show tables;

	Otra formar

	$ mysql -u root -p loudacre

## Comandos Sqoop

Comandos sqoop

Listar tablas
	$ sqoop list-tables --connect jdbc:mysql://localhost/loudacre --username training --password training
 
Importar una tabla

 * Importar todas las tablas de una BBDD
	$ sqoop import-all-tables --connect jdbc:mysql://localhost/loudacre --username training --password training --warehouse-dir /loudacre
 
 * Importar una tabla
 	$ sqoop import --table device --connect jdbc:mysql://localhost/loudacre --username training --password training 

 * Importación parcial: campos
	$ sqoop import --table device --connect jdbc:mysql://localhost/loudacre --username training --password training  --columns "device_num"

 * Importación parcial: filter
	$ sqoop import --table device --connect jdbc:mysql://localhost/loudacre --username training --password training --where "device_name like '%Sorrento%'"

Sqoop genera ficheros de textos separados por coma. Se puede cambiar, en este caso es un tabulador:
	$ sqoop import --table device --connect jdbc:mysql://localhost/loudacre --username training --password training --fields-terminated-by "\t"	


Exportar datos a MySQL desde HDFS
	$ sqoop export --connect jdbc:mysql://dbhost/loudacre --username training --password training --export-dir /loudacre/recommender_output --update-mode allowinsert --table product_recommendations
