package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_TransForm_Max_MaxBy {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口中获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("mospade202", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });


        // 3.将相同id的数据聚和到一块
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);
//        map.keyBy(value -> value.getId())


        //4.使用聚和算子
        SingleOutputStreamOperator<WaterSensor> max = keyedStream.max("vc");

        SingleOutputStreamOperator<WaterSensor> maxBy = keyedStream.maxBy("vc", true);

        SingleOutputStreamOperator<WaterSensor> maxByFlase = keyedStream.maxBy("vc", false);

        System.out.println("---------------------");

        max.print("Max");

        maxBy.print("MaxBy");

        maxByFlase.print("MaxBy-False");

        env.execute();
    }
}
