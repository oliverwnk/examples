# Example using KafkaSinglePortExactlyOnceOutputOperator (Kafka 0.9 API)

This application shows the benefits of exactly-once OutputOperators by
comparing the queues of two different Kafka topics using
[KafkaSinglePortOutputOperator](https://github.com/apache/apex-malhar/blob/master/kafka/src/main/java/org/apache/apex/malhar/kafka/KafkaSinglePortOutputOperator.java)
for writing to one topic and [KafkaSinglePortExactlyOnceOutputOperator](https://github.com/apache/apex-malhar/blob/master/kafka/src/main/java/org/apache/apex/malhar/kafka/KafkaSinglePortExactlyOnceOutputOperator.java)
for writing to an other.

The Application reads lines from a file on HDFS using LineByLineFileInputOperator
and passes them to the PassthroughFailOperator which emits lines to both of the KafkaOutputOperators.

To produce an exactly-once scenario the PassthroughFailOperator kills itself after a certain number
of processed lines by throwing an exception. YARN will deploy the Operator in a new container,
hence not checkpointed tuples will be passed to the OutputOperators more than once.

Comparing the two topics in Kafka or using the included CompareKafkaTopicsApplication will show how
KafkaSinglePortExactlyOnceOutputOperator handled this scenario.

**NOTE to Kafka:**

A running Kafka service is needed to run the Application.
A local single-node instance can easily be deployed (see [kafka.apache.org/quickstart](https://kafka.apache.org/quickstart)).

Create two topics 'exaclty-once' and 'not-exactly-once'. (Change values in properties.xml when using different topic names).

Change properties in Application if Kafka is not running locally.



**Follow these steps to run this application:**

Copy inputLines.txt from 'src/main/resources/inputLines.txt' to your HDFS '/kafka_exactly/inputLines.txt'

```
shell> hdfs dfs -put examples/tutorials/kafkaexactlyonce/src/main/resources/inputLines.txt /kafka_exactly/
```

Make sure Kafka is running.

Run the Application.

**Compare outcome:**

CompareKafkaTopicsApplication uses KafkaSinglePortInputOperator to read
the written messages from Kafka topics and emit a Map<String, ArrayList<String>>
with Kafka topics as the key to stdtout using [ConsoleOutputOperator](https://github.com/apache/apex-malhar/blob/master/library/src/main/java/com/datatorrent/lib/io/ConsoleOutputOperator.java).
