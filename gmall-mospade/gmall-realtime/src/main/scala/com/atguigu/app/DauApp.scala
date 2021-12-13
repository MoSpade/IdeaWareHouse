package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_STARTUP, ssc)

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        val times: String = sdf.format(new Date(startUpLog.ts))

        startUpLog.logDate = times.split(" ")(0)

        startUpLog.logHour = times.split(" ")(1)

        startUpLog
      })
    })
    startUpLogDStream.cache()

    //5.批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream)

    filterByRedisDStream.cache()

    startUpLogDStream.count().print()

    filterByRedisDStream.count().print()

    //6.批次内去重

    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    filterByGroupDStream.count().print()


    //7.将去重的(mid)保存在redis
    DauHandler.saveToRedis(filterByRedisDStream)

    //8.将去重后的明细数据保存在Hbase
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("mospade202,mospade203,mospade204:2181"))
    })



    //foreach没有输出
    /*    kafkaDStream.foreachRDD(rdd => {
            rdd.foreach(record => {
                println(record.value())
              })
          })*/

    //消费Kafka数据并打印测试
    /*val value: DStream[String] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        record.value()
      })
    })
    value.print()*/

    ssc.start()
    ssc.awaitTermination()
  }
}
