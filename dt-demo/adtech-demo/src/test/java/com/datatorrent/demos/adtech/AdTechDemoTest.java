/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.adtech;

import java.net.URI;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.lib.util.TestUtils.TestInfo;

import info.batey.kafka.unit.KafkaUnitRule;
import kafka.producer.KeyedMessage;

/**
 * This test requires a gateway running on the local machine.
 */
public class AdTechDemoTest
{
  public TestInfo testMeta = new TestInfo();
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(2181, 9092);

  @Rule
  public TestRule chain = RuleChain.outerRule(testMeta).around(kafkaUnitRule);


  @Test
  public void applicationTest() throws Exception
  {
    String kafkaTopic = "adTech";
    String testMessage = "fox network,6,mcdonalds,2,,24,4.450882117544054,0.0,1,0,1491250485306";

    kafkaUnitRule.getKafkaUnit().createTopic(kafkaTopic);
    KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(kafkaTopic, kafkaTopic, testMessage);
    kafkaUnitRule.getKafkaUnit().sendMessages(keyedMessage);

    String gatewayConnectAddress = "localhost:9090";
    URI uri = URI.create("ws://" + gatewayConnectAddress + "/pubsub");

    AdTechApplication adTechDemo = new AdTechApplication();

    Configuration conf = new Configuration(false);
    conf.addResource("META-INF/properties.xml");
    conf.set("dt.attr.GATEWAY_CONNECT_ADDRESS", gatewayConnectAddress);
    conf.set("dt.application.AdsDimensionsDemoGeneric.operator.Store.fileStore.basePathPrefix",
        testMeta.getDir());
    conf.set("dt.application.adTechDemo.operator.kafkaInput.prop.clusters", "localhost:9092");

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(adTechDemo, conf);
    lma.cloneDAG();
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    String query = SchemaUtils.jarResourceFileToString("adsquery.json");

    PubSubWebSocketAppDataQuery pubSubInput = new PubSubWebSocketAppDataQuery();

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    TestUtils.setSink(pubSubInput.outputPort, sink);

    pubSubInput.setTopic("AdTechDemoQueryResult");
    pubSubInput.setUri(uri);
    pubSubInput.setup(null);
    pubSubInput.activate(null);

    PubSubWebSocketOutputOperator<String> pubSubOutput = new PubSubWebSocketOutputOperator<String>();
    pubSubOutput.setTopic("AdTechDemo");
    pubSubOutput.setUri(uri);
    pubSubOutput.setup(null);

    pubSubOutput.beginWindow(0);
    pubSubInput.beginWindow(0);

    Thread.sleep(10000);

    pubSubOutput.input.put(query);

    Thread.sleep(10000);

    pubSubInput.outputPort.flush(Integer.MAX_VALUE);

    Assert.assertEquals(1, sink.collectedTuples.size());
    String resultJSON = sink.collectedTuples.get(0).toString();
    JSONObject result = new JSONObject(resultJSON);
    JSONArray array = result.getJSONArray("data");
    JSONObject val = array.getJSONObject(0);
    Assert.assertEquals(1, array.length());

    //Assert.assertEquals("3.00", val.get("revenue:SUM"));
    //Assert.assertEquals("5.00", val.get("cost:SUM"));
    //Assert.assertEquals("10", val.get("clicks:SUM"));
    //Assert.assertEquals("5", val.get("impressions:SUM"));

    pubSubInput.deactivate();

    pubSubOutput.teardown();
    pubSubInput.teardown();
  }

  private static final Logger LOG = LoggerFactory.getLogger(AdTechDemoTest.class);
}
