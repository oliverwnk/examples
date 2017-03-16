package com.datatorrent.demos.adtech;

import java.util.List;

import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.hadoop.conf.Configuration;

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
    KafkaSinglePortOutputOperator<String, String> kafkaOutput = dag.addOperator("kafkaOutput",
        KafkaSinglePortOutputOperator.class);

    dag.addStream("inputData", inputOperator.getOutputPort(), kafkaOutput.inputPort)
      .setLocality(Locality.CONTAINER_LOCAL);
  }
}
