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
    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);

    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    input.advertiserName = advertisers;
    input.setEventSchemaJSON(eventSchema);
    inputOperator = input;

    KafkaSinglePortExactlyOnceOutputOperator<String> kafkaOutput = dag.addOperator("kafkaOutput",
        KafkaSinglePortExactlyOnceOutputOperator.class);

    //Set properties for KafkaSinglePortExactlyOnceOutputOperator
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node18.morado.com:9092");
    // Kafka's Key field is used by KafkaSinglePortExactlyOnceOutputOperator to implement exactly once property
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaOutput.setProperties(props);

    dag.addStream("inputData", inputOperator.getOutputPort(), kafkaOutput.inputPort).setLocality(Locality.CONTAINER_LOCAL);
  }
}
