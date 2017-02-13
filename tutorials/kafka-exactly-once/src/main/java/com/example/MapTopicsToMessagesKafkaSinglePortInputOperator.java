package com.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.datatorrent.api.DefaultOutputPort;

/**
 * Reads messages from Kafka topics and maps them to topic as the keys
 */
public class MapTopicsToMessagesKafkaSinglePortInputOperator extends KafkaSinglePortInputOperator
{
  private transient Map<String, ArrayList<String>> messagesMap = new HashMap<>();

  /**
   * This output port emits tuples extracted from Kafka messages.
   */
  public final transient DefaultOutputPort<Map<String, ArrayList<String>>> outputPort = new DefaultOutputPort<>();

  @Override
  protected void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message)
  {
    ArrayList<String> list;
    if (messagesMap.get(message.topic()) == null) {
      list = new ArrayList<>();
    } else {
      list = messagesMap.get(message.topic());
    }

    list.add(new String(message.value()));
    messagesMap.put(message.topic(), list);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    outputPort.emit(messagesMap);
  }
}
