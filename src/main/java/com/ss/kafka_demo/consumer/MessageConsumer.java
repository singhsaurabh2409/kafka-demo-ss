package com.ss.kafka_demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class MessageConsumer {

  @Value("topic.name")
  private String topic;

  @Autowired
  private ConsumerFactory<String, String> consumerFactory;

  @KafkaListener(topics="users",groupId = "users")
  public void onMessage(String message){
    log.info("Message in consumer : {}", message);
  }

  public void consumeMessages(long startOffset, long endOffset) {
    List<String> messages = new ArrayList<>();

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
            consumerFactory.getConfigurationProperties())) {
      List<TopicPartition> topicPartitions = consumer
              .partitionsFor(topic)
              .stream()
              .map(info -> new TopicPartition(topic, info.partition()))
              .collect(Collectors.toList());

      consumer.assign(topicPartitions);

      topicPartitions.forEach(topicPartition -> consumer.seek(topicPartition, startOffset));

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

      messages = records
              .records(topicPartitions.get(0))
              .stream()
              .filter(record -> record.offset() < endOffset)
              .peek(record -> log.info("Consumed String message: {}, offset: {}", record.value(),
                      record.offset()))
              .map(ConsumerRecord::value)
              .collect(Collectors.toList());

      consumer.commitAsync();

    } catch (Exception e) {
      log.error("Error while consuming String messages: {}", e.getMessage(), e);
      throw new RuntimeException("Error while consuming messages", e);
    }

  }
}
