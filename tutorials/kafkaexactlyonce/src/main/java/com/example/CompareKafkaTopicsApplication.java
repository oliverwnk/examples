package com.example;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

/**
 * Created by oliver on 2/2/17.
 */
@ApplicationAnnotation(name = "CompareKafkaTopicsApp")
public class CompareKafkaTopicsApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    MapTopicsToMessagesKafkaSinglePortInputOperator kafkaInputOperator = dag.addOperator("kafkaInput", MapTopicsToMessagesKafkaSinglePortInputOperator.class);
    //ByteArrayToStringConverterOperator bytesToStringOperator = dag.addOperator("bytesToString", ByteArrayToStringConverterOperator.class);
    ConsoleOutputOperator consoleOperator = dag.addOperator("consoleOperator", ConsoleOutputOperator.class);

    kafkaInputOperator.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());

    dag.addStream("kafkaMessages", kafkaInputOperator.outputPort, consoleOperator.input);
  }
}