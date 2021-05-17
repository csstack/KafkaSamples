package com.css.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class KafkaProducerApp {
    public static void main(String[] args) {
        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer(props);
        try {
            IntStream.range(0, 150).forEach(v-> produce(v, producer));
        } finally {
            producer.close();
        }
    }

    private static void produce(int value, Producer producer) {
        System.out.println("Producing :: " + value);
        ProducerRecord<String, String> record = new ProducerRecord("my_topic",
                Integer.toString(value), "Message:"+ value);
        producer.send(record);
    }
}
