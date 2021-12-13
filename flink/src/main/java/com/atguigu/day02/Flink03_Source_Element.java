package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_Element {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.fromElements(1,2,3,4,5).print();

        env.execute();
    }
}
