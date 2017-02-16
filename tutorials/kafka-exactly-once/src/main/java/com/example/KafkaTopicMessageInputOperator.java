package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Created by oliver on 2/14/17.
 */
public class KafkaTopicMessageInputOperator extends AbstractKafkaInputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicMessageInputOperator.class);
  /**
   * This output port emits (message.topic, message.value) pairs extracted from Kafka messages.
   */
  public final transient DefaultOutputPort<KeyValPair<String, String>> outputPort = new DefaultOutputPort<>();

  @Override
  protected void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message)
  {
    LOG.debug("foo " + message.topic());
    KeyValPair<String, String> pair= new KeyValPair<>(message.topic(), new String(message.value()));
    outputPort.emit(pair);
  }

}

