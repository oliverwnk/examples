package com.datatorrent.demos.adtech;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.ConsoleOutputOperator;

/**
 * Created by oliver on 2/27/17.
 */
@ApplicationAnnotation(name = "adTechDemo")
public class AdTechApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafkaInput = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);
    CsvParser csvParser = dag.addOperator("csvParser", new CsvParser());
    csvParser.setSchema(SchemaUtils.jarResourceFileToString("csvSchema.json"));
    ConsoleOutputOperator console = dag.addOperator("console", ConsoleOutputOperator.class);

    dag.addStream("kafkaInputStream", kafkaInput.outputPort, csvParser.in);
    dag.addStream("parsedAdInfoObj", csvParser.out, console.input);
  }
}
