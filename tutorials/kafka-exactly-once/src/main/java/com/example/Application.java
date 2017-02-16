/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.example;

import java.util.Properties;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortExactlyOnceOutputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import kafka.consumer.ConsumerConfig;

/**
 * This application shows the benefits of exactly-once OutputOperators by
 * comparing the queues of two different Kafka topics using KafkaSinglePortOutputOperator for writing to one topic
 * and KafkaSinglePortExactlyOnceOutputOperator for writing to an other.
 *
 * The Application reads lines from a file on HDFS using LineByLineFileInputOperator
 * and passes them to the PassthroughFailOperator which emits lines to both of the KafkaOutputOperators. (Kafka 0.9 API)
 */

@ApplicationAnnotation(name = "KafkaExactlyOnce")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    BatchSequenceGenerator sequenceGenerator = dag.addOperator("sequenceGenerator", BatchSequenceGenerator.class);
    KafkaSinglePortExactlyOnceOutputOperator<String> kafkaExactlyOnceOutputOperator =
        dag.addOperator("kafkaExactlyOnceOutputOperator", KafkaSinglePortExactlyOnceOutputOperator.class);
    KafkaSinglePortOutputOperator kafkaOutputOperator =
        dag.addOperator("kafkaOutputOperator", KafkaSinglePortOutputOperator.class);
    PassthroughFailOperator passthroughFailOperator = dag.addOperator("passthrough", PassthroughFailOperator.class);

    //properties for kafka output operators
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Kafka's Key field is used by KafkaSinglePortExactlyOnceOutputOperator to implement exactly once property
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    kafkaExactlyOnceOutputOperator.setProperties(props);
    kafkaOutputOperator.setProperties(props);

    dag.addStream("sequenceToPassthrough", sequenceGenerator.out, passthroughFailOperator.input);
    dag.addStream("linesToKafka", passthroughFailOperator.output, kafkaOutputOperator.inputPort,
      kafkaExactlyOnceOutputOperator.inputPort);

    KafkaTopicMessageInputOperator kafkaInput = dag.addOperator("kafkaInput", KafkaTopicMessageInputOperator.class);
    MessagesValidationToFile validationToFile = dag.addOperator("validationToFile", MessagesValidationToFile.class);
    kafkaInput.setInitialOffset(KafkaTopicMessageInputOperator.InitialOffset.EARLIEST.name());

    dag.addStream("messages", kafkaInput.outputPort, validationToFile.input);
  }
}
