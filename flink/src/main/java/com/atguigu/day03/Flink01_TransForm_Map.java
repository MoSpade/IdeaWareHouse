package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_TransForm_Map {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> streamSource = env.fromElements(1, 3, 5, 7);

        //todo map算子,匿名内部类实现
/*        streamSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * integer;
            }
        }).print();*/

        //todo map算子,lambda实现
//        streamSource.map(ele->ele * ele).print();

        //todo map算子,自定义静态内部类实现
        streamSource.map(new MyMapFunction()).print();

        env.execute();

    }

    private static class MyMapFunction implements MapFunction<Integer,Integer>{
        @Override
        public Integer map(Integer integer) throws Exception {
            return integer * integer;
        }
    }
}
