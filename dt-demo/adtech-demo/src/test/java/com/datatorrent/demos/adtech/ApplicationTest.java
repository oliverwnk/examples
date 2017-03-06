/**
 * Put your copyright and license info here.
 */
package com.datatorrent.demos.adtech;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

import info.batey.kafka.unit.KafkaUnit;
import info.batey.kafka.unit.KafkaUnitRule;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{

  private static final int zkPort = 2181;
  private static final int brokerPort = 9092;

  // broker port must match properties.xml
  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      createTopics();

      // run app asynchronously
      Configuration conf = getConfig();
      LocalMode lma = LocalMode.newInstance();
      lma.prepareDAG(new DataToKafkaGenApplication(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      LocalMode lmaAdTech = LocalMode.newInstance();
      lma.prepareDAG(new AdTechApplication(), conf);
      LocalMode.Controller lcAdTech = lma.getController();
      lcAdTech.runAsync();

      Thread.sleep(10000);
      lc.shutdown();
      Thread.sleep(10000);
      lcAdTech.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private Configuration getConfig()
  {
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    conf.set("dt.application.adTechDemo.operator.Enrich.prop.store.fileName", "src/main/resources/enrichMapping.txt");
    return conf;
  }

  private void createTopics() throws Exception
  {
    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    ku.createTopic("adtech");
  }
}