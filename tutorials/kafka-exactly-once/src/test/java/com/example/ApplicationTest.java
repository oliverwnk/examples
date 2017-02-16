package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.List;
import java.util.concurrent.TimeoutException;

import javax.validation.ConstraintViolationException;
import javax.validation.constraints.NotNull;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import org.junit.Test;

import info.batey.kafka.unit.KafkaUnitRule;
import info.batey.kafka.unit.KafkaUnit;

import kafka.producer.KeyedMessage;

import com.datatorrent.api.LocalMode;

import static org.junit.Assert.assertTrue;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String directory = "target/kafka_exactly";

  private static final String FILE_NAME = "messages.txt";

  private static final int zkPort = 2181;
  private static final int  brokerPort = 9092;
  private static final String BROKER = "localhost:" + brokerPort;

  //private static final String FILE_PATH = FILE_DIR + "/" + FILE_NAME + ".0";     // first part



  // broker port must match properties.xml
  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);


  @Test
  public void testApplication() throws IOException, Exception {
    try {
      cleanup();
      createTopics();

      // run app asynchronously; terminate after results are checked
      LocalMode.Controller lc = asyncRun();

      // get messages from Kafka topic and compare with input
      Thread.sleep(10000);
      lc.shutdown();
      //checkOutput();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private void cleanup()
  {
    try {
      FileUtils.deleteDirectory(new File(directory));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private LocalMode.Controller asyncRun() throws Exception {
    Configuration conf = getConfig();
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    return lc;
  }

  private Configuration getConfig() {
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      conf.set("dt.application.KafkaExactlyOnce.operator.passthrough.prop.directoryPath", directory);
      conf.set("dt.application.KafkaExactlyOnce.operator.validationToFile.prop.filePath", directory);
      return conf;
  }

  private void createTopics() throws Exception {
    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    ku.createTopic("exactly-once");
    ku.createTopic("at-least-once");
  }


  private void checkOutput() throws IOException
  {
    //TODO finalize file then put this method in test and test
    String validationOutput;
    String tuplesUntilKill = String.valueOf("5");
    FileInputStream inputStream = new FileInputStream(directory + "/validation.txt_5.1487186022266.tmp");
    try {
      validationOutput = IOUtils.toString(inputStream);
    } finally {
      inputStream.close();
    }
    Assert.assertEquals(validationOutput, "Duplicates: exactly-once: 0, at-least-once: " + tuplesUntilKill);
  }
}
