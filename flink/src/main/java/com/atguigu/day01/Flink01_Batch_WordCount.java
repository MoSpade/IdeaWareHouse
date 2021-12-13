package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.从文件读取数据
        DataSource<String> dataSource = env.readTextFile("flink/input/words.txt");

        //先flatMap(按照空格切分,然后打散成Tuple2元组(word,1)->reduceByKey(将相同单词的数据聚合到一块并做累加)-> 打印到控制台)
        //3.将数据按照空格切分,然后组成Tuple2元组
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = dataSource.flatMap(new MyFlatMap());

        //4.将相同的单词聚合到一块
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //5.将单词的个数做累加
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //6.打印到控制台
        result.print();
    }

    private static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>> {

        /**
         *
         * @param s
         * @param collector 采集器,将数据采集起来发送到下游
         * @throws Exception
         */
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //将数据按照空格切分
            String[] words = s.split(" ");

            //遍历出每一个单词
            for (String word : words) {
                //将单词组成Tuple2元组通过采集器发送至下游
                collector.collect(new Tuple2<>(word,1));
                //这个方法实质也是new了一个对象
                //collector.collect(Tuple2.of(word,1));

            }

        }
    }
}
