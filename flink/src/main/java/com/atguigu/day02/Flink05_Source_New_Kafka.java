package com.atguigu.day02;

import javafx.beans.binding.StringExpression;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink05_Source_New_Kafka {
    public static void main(String[] args) throws Exception {

        //新版本创建Kafka相关配置
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mospade202:9092,mospade203:9092,mospade204:9092")
                .setTopics("sensor")
                .setGroupId("Flink04_Source_Kafka")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        DataStreamSource<String> kafka_source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        kafka_source.print();

        env.execute();

    }
}
