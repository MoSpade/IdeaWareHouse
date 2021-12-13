package com.atguigu.day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_ExecutionMode_WordCount {
    public static void main(String[] args) throws Exception {

        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //todo 设置执行模式

        //不设置的情况下,默认为流模式

        //自动模式
            //在有界流模式下为批处理
            //在无界流模式下为流处理
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //批模式
            //在无界流模式下,不被允许
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //流模式
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //从文件中获取数据,有界流
        DataStreamSource<String> streamSource = env.readTextFile("flink/input/words.txt");

        //从端口中获取数据,无界流
//        DataStreamSource<String> streamSource = env.socketTextStream("mospade202", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");

                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = streamOperator.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        sum.print();

        env.execute();

    }
}
