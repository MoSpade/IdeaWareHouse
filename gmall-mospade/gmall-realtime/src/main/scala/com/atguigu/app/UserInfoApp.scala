package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //3.获取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER,ssc)

    //4.将数据写入redis
    kafkaDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(partition =>{

        //获取redis连接
        val jedis: Jedis = new Jedis("mospade202",6379)
        partition.foreach(record =>{
          //将UserInfo表的字符串写入redis
          val userInfo: UserInfo = JSON.parseObject(record.value(),classOf[UserInfo])
          val userInfoRedisKey: String = "userInfo" + userInfo.id
          jedis.set(userInfoRedisKey,record.value())
        })
        jedis.close()
      })
    })

    //5.将数据转为样例类
    val userInfoDStream: DStream[UserInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })
    userInfoDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
