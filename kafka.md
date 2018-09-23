###KAFKA

Apache Kafka es un proyecto de intermediación de mensajes de código abierto desarrollado por la Apache Software Foundation escrito en Scala.
El proyecto tiene como objetivo proporcionar una plataforma unificada, de alto rendimiento y de baja latencia para la manipulación en tiempo real de fuentes de datos. 

Apache Kafka es un sistema de intermediación de **mensajes**. Puede verse como una cola de mensajes, bajo el patrón **publicación-suscripción**. Es ampliamente utilizado para la ingesta de datos. Ofrece un sistema sistema persistente, escalable, replicado y tolerante a fallos. Se usa para el procesamiento de datos en streaming


**Topic (tema)**: Categorías en las que clasificar los mensajes enviados a Kafka.T

**Producer (productor)** Clientes conectados responsables de publicar los mensajes. Estos mensajes son publicados sobre uno o varios topics.

**Consumer (consumidor)** Clientes conectados suscritos a uno o varios topics encargados de consumir los mensajes.

**Broker (nodos)**: Nodos que forman el cluster.

Un cluster de kafka consiste en uno o mas brokers (servidores) en el que  se ejecuta un daemon. Kaffa depende de Apache de zookeeper para la coordinacion en el cluster

KAFKA HOME /home/arturo/desarrollo/kafka_2.11-2.0.0

start server kakfa

#zookeper
	
	./bin/zookeeper-server-start.sh config/zookeeper.properties

#Servidor kafka
	
	./bin/kafka-server-start.sh config/server.properties


#Nuevo topico "test"
	
	./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

#Productor de mensajes: Mandar mensajes a test
	
	./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

#Consumidor de mensajes
	
	./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning


#Command to run kafka-connect with a file source connector and a task.

./bin/connect-standalone.sh config/connect-standalone.properties /home/arturo/desarrollo/curso_spark/files/kafka/connect-file-source.properties

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic file-source-test --from-beginning


./bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties config/connect-file-sink.properties
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic connect-test --from-beginning


_______________**connect-file-source.properties**

#A logical name for your task
name=demo-file-source

#Class of connector to use
connector.class=FileStreamSource

#Number of parallel tasks to launch. Provides scalability
tasks.max=1

#Local file to monitor for new input
file=/home/arturo/desarrollo/files/file-test.txt

#Topic to publish data to.
topic=file-source-test
____________________________________________________________
