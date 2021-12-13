package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //从文件中获取数据
        env.readTextFile("flink/input/sensor.txt").print();

        env.execute();

    }
}
