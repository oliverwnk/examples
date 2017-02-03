/**
 * Put your copyright and license info here.
 */
package com.example;

import java.util.Properties;

import org.apache.apex.malhar.kafka.KafkaSinglePortExactlyOnceOutputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "KafkaExactlyOnceApp")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    //test
//    WindowIdInputOperator windowIdInput = dag.addOperator("inputOperator", WindowIdInputOperator.class);

    dag.getAttributes().put(DAG.STREAMING_WINDOW_SIZE_MILLIS, 200);
    //read lines from incoming files in 'inputDirectory' (set in properties.xml)
    LineByLineFileInputOperator lineInputOperator =
        dag.addOperator("lineInputOperator", LineByLineFileInputOperator.class);
    KafkaSinglePortExactlyOnceOutputOperator<String> kafkaExactlyOnceOutputOperator =
        dag.addOperator("kafkaExactlyOnceOutputOperator", KafkaSinglePortExactlyOnceOutputOperator.class);
    KafkaSinglePortOutputOperator kafkaOutputOperator =
        dag.addOperator("kafkaOutputOperator", KafkaSinglePortOutputOperator.class);
    ConsoleOutputOperator consoleOutputOperator =
        dag.addOperator("consoleOutputOperator", ConsoleOutputOperator.class);
    PassthroughFailOperator passthroughFailOperator = dag.addOperator("passthrough", PassthroughFailOperator.class);

    //properties for kafka output operators
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Kafka's Key field is used by KafkaSinglePortExactlyOnceOutputOperator to implement exactly once property
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    //kafkaOutOperator.setTopic("exactly-once");
    kafkaExactlyOnceOutputOperator.setProperties(props);
    kafkaOutputOperator.setProperties(props);

    dag.addStream("linesToPassthrough", lineInputOperator.output, passthroughFailOperator.input);
    dag.addStream("linesToKafka", passthroughFailOperator.output, kafkaOutputOperator.inputPort,
        kafkaExactlyOnceOutputOperator.inputPort, consoleOutputOperator.input);

  }
}
