/**
 * Put your copyright and license info here.
 */
package com.datatorrent.demos.adtech;

import java.util.List;
import java.util.Properties;

import org.apache.apex.malhar.kafka.KafkaSinglePortExactlyOnceOutputOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;

@ApplicationAnnotation(name = "KafkaDataGenerator")
public class DataToKafkaGenApplication implements StreamingApplication
{
  public static final String EVENT_SCHEMA = "adsGenericEventSchema.json";

  public String eventSchemaLocation = EVENT_SCHEMA;
  public List<Object> advertisers;
  public InputItemGenerator inputOperator;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build
    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);

    if (inputOperator == null) {
      InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
      input.advertiserName = advertisers;
      input.setEventSchemaJSON(eventSchema);
      inputOperator = input;
    } else {
      dag.addOperator("InputGenerator", inputOperator);
    }

    KafkaSinglePortExactlyOnceOutputOperator<String> kafkaOutput = dag.addOperator("kafkaOutput",
      KafkaSinglePortExactlyOnceOutputOperator.class);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Kafka's Key field is used by KafkaSinglePortExactlyOnceOutputOperator to implement exactly once property
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    kafkaOutput.setProperties(props);

    //ConsoleOutputOperator cons = dag.addOperator("console", ConsoleOutputOperator.class);
    dag.addStream("inputData", inputOperator.getOutputPort(), kafkaOutput.inputPort).setLocality(Locality.CONTAINER_LOCAL);
  }
}
