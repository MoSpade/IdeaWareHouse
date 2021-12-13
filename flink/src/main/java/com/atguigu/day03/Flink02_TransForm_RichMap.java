package com.atguigu.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_TransForm_RichMap {
    public static void main(String[] args){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(5);

        DataStreamSource<Integer> streamSource = env.fromElements(1, 3, 5, 7, 9);

        SingleOutputStreamOperator<Integer> map = streamSource.map(new MyRichFunction());
    }

    private static class MyRichFunction extends RichMapFunction<Integer,Integer> {

        @Override
        public Integer map(Integer integer) throws Exception {
            return null;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
