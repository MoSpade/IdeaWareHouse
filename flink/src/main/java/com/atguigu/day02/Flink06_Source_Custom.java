package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink06_Source_Custom {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource()).setParallelism(2);

        streamSource.print();

        env.execute();


    }

    private static class MySource implements ParallelSourceFunction<WaterSensor> {

        private Boolean isRunning = true;
        private Random random = new Random();
        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            while(isRunning){
                sourceContext.collect(new WaterSensor("sensor" + random.nextInt(100),System.currentTimeMillis(),random.nextInt(1000)));
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;

        }
    }
}
