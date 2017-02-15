package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.datatorrent.api.DefaultOutputPort;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Reads messages from Kafka topics and maps them to topic as the keys
 */
public class TopicsToMessages extends KafkaSinglePortInputOperator
{
  private transient Map<String, List<String>> messagesMap = new HashMap<>();


  private  String filePath;
  int numTuples = 0;


  /**
   * This output port emits tuples extracted from Kafka messages.
   */
  public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();

  @Override
  protected void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message)
  {
    numTuples++;
    final String topic = message.topic();
    List<String> list = messagesMap.get(topic);
    if (list == null) {
      list = new ArrayList<>();
      messagesMap.put(topic, list);
    }
    list.add(new String(message.value()));
    //messagesMap.put(topic, list);

    List<String> exactlyList = messagesMap.get("exactly-once");
    List<String> atLeastList = messagesMap.get("not-exactly-once");
    if (true) {

      //Set<String> exactlySet = new HashSet<String>(exactlyList);
      //Set<String> atLeastSet = new HashSet<String>(atLeastList);

      //int numDuplicatesExactly = exactlyList.size() - exactlySet.size();
      //int numDuplicatesAtLeast = atLeastList.size() - atLeastSet.size();

      String outputString = "Duplicates: exactly-once: ";// + numDuplicatesExactly + ", at-least-once: " + numDuplicatesAtLeast;
      Path filePathObj = new Path(filePath);

      try {
        FileSystem hdfs = FileSystem.newInstance(new Configuration());
        if (hdfs.exists(filePathObj)) {
          hdfs.delete(filePathObj, false);
        }
        hdfs.createNewFile(filePathObj);
        byte[] bytes = outputString.getBytes();
        FSDataOutputStream fsDataOutputStream = hdfs.create(filePathObj);
        fsDataOutputStream.write(bytes);
        fsDataOutputStream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  @Override
  public void endWindow()
  {
    super.endWindow();

  }

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

}
