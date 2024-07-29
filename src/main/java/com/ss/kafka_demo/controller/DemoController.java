package com.ss.kafka_demo.controller;

import com.ss.kafka_demo.consumer.MessageConsumer;
import com.ss.kafka_demo.producer.MessageProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
public class DemoController {


  private final MessageProducer messageProducer;
  private final MessageConsumer messageConsumer;
  public DemoController(MessageProducer messageProducer, MessageConsumer messageConsumer) {
    this.messageProducer = messageProducer;
    this.messageConsumer = messageConsumer;
  }

  @PostMapping("/send")
  public String sendMessage(@RequestParam("message") String message) {
    messageProducer.sendMessage(message);
    log.info(message);
    return "Message sent: " + message;

  }

  @GetMapping("/hello")
  public String sayHello(){
    return "Hello !!! ";
  }

  @PostMapping("/setOffsets")
  public String setOffsets(@RequestParam long startOffset, @RequestParam long endOffset){
    messageConsumer.consumeMessages(startOffset,endOffset);
    return "Consumed messages as requested !!!";
  }



}
