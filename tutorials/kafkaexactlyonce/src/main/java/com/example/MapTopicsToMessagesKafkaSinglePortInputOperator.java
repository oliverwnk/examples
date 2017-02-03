package com.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by oliver on 2/2/17.
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
