package com.atguigu.handler

import java.lang
import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {


  /**
    * 批次内去重
    *
    * @param filterByRedisDStream
    * @return
    */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //1.先将数据转为k,v格式的数据
    val midWithLogDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(startUplog => {
      ((startUplog.mid, startUplog.logDate), startUplog)
    })
    //2.按照key进行聚合
    val midWithLogDateIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midWithLogDateToLogDStream.groupByKey()

    //3.对相同key的数据进行排序
    val midWithLogDateToListLogDStream: DStream[((String, String), List[StartUpLog])] = midWithLogDateIterLogDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //4.获取List集合中每一个元素
    val value: DStream[StartUpLog] = midWithLogDateToListLogDStream.flatMap(_._2)
    value
  }


  /**
    * 批次间去重
    *
    * @param startUpLogDStream
    * @return
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    val value: DStream[StartUpLog] = startUpLogDStream.filter(startUplog => {
      val jedis: Jedis = new Jedis("mospade202", 6379)

      val redisKey: String = "DAU:" + startUplog.logDate

      val boolean: Boolean = jedis.sismember(redisKey, startUplog.mid)

      jedis.close()

      !boolean
    })
    value
  }

  /**
    * 将去重的(mid)保存在redis
    *
    * @param startUpLogDStream
    * @return
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val jedis: Jedis = new Jedis("mospade202", 6379)
        partition.foreach(log => {
          val redisKey: String = "DAU:" + log.logDate
          jedis.sadd(redisKey, log.mid)
        })
        jedis.close()
      })
    })
  }

}
