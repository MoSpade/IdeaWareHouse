package com.atguigu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MykafkaSender {

    public static KafkaProducer<String,String> kafkaProducer =null;

    public static KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();

        properties.put("bootstrap.servers","mospade202:9092,mospade203:9092,mospade204:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer =null;

        producer = new KafkaProducer<String,String>(properties);

        return producer;

    }

    public static void send(String topic,String msg){
        if(kafkaProducer==null){
            kafkaProducer=createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String,String>(topic,msg));
    }
}
