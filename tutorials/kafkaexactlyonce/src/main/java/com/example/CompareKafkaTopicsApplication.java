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

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

/**
 * This application uses {@link MapTopicsToMessagesKafkaSinglePortInputOperator} to read
 * the written messages from Kafka topics and emit a Map<String, ArrayList<String>>
 * with Kafka topics as the key to ConsoleOutputOperator.
 */
@ApplicationAnnotation(name = "CompareKafkaTopicsApp")
public class CompareKafkaTopicsApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    MapTopicsToMessagesKafkaSinglePortInputOperator kafkaInputOperator =
        dag.addOperator("kafkaInput", MapTopicsToMessagesKafkaSinglePortInputOperator.class);
    ConsoleOutputOperator consoleOperator = dag.addOperator("consoleOperator", ConsoleOutputOperator.class);

    kafkaInputOperator.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());

    dag.addStream("kafkaMessages", kafkaInputOperator.outputPort, consoleOperator.input);
  }
}
