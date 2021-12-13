package com.atguigu.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Flink04_Source_Old_Kafka {
    public static void main(String[] args) throws Exception {

        //老版本创建Kafka相关配置
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers","mospade202:9092,mospade203:9092,mospade204:9092");
        properties.setProperty("group.id","Flink04_Source_Kafka");
        properties.setProperty("auto.offset.reset","latest");

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //addSource方式
        DataStreamSource<String> sensor = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));

        sensor.print();

        env.execute();


    }
}
