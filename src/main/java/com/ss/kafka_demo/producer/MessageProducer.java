package com.ss.kafka_demo.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class MessageProducer {

  @Value("topic.name")
  private String topic;


  private final KafkaTemplate<String,String> kafkaTemplate;

  public MessageProducer(KafkaTemplate<String,String> kafkaTemplate){
    this.kafkaTemplate=kafkaTemplate;
  }

  public void sendMessage(String message){
    kafkaTemplate.send(topic,message);
  }
}
