package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //3.消费kafka数据
    //获取订单表的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    //获取明细表的数据
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,ssc)

    //4.分别将两张表的数据转为样例类
    //订单表
    val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1)

        (orderInfo.id, orderInfo)
      })
    })

    //订单明细表
    val orderDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })
    //打印测试
    //orderInfoDStream.print()
    //orderDetailDStream.print()

    //5.对两条流进行 join,内连接不保留数据,所以会漏数据
//    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
//    value.print();

    //5.对两条流进行full join
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.利用缓存的方式来处理因网络延迟所带来的数据丢失问题
    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(partition => {

      //隐式转换
      implicit val formats = org.json4s.DefaultFormats

      //创建结果集合
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建redis连接
      val jedis: Jedis = new Jedis("mospade202", 6379)

      partition.foreach { case (orderId, (infoOpt, detailOpt)) =>

        //orderInfoRedisKey
        val orderInfoRedisKey: String = "orderInfo" + orderId

        //orderDetailRedisKey
        val orderDetailRedisKey: String = "orderDetail" + orderId

        //todo 判断orderinfo数据是否存在
        if (infoOpt.isDefined) {
          //orderInfo存在
          val orderInfo: OrderInfo = infoOpt.get
          //todo 判断orderDetail数据是否存在
          if (detailOpt.isDefined) {
            //orderDetail存在
            val orderDetail: OrderDetail = detailOpt.get

            val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)

            //将关联后的样例类存入结果集合
            details.add(detail)
          }
          //todo 将orderInfo数据写入缓存(redis)


          val orderInfoJson: String = Serialization.write(orderInfo)

          jedis.set(orderInfoRedisKey, orderInfoJson)

          //对数据设置一个过期时间,防止内存不够
          jedis.expire(orderInfoRedisKey, 20)

          //todo 查询对方缓存中是否有对应的orderDetail数据
          //判断rediskey是否存在
          if (jedis.exists(orderDetailRedisKey)) {
            //取出orderDetail数据
            val detailsJsonStr: util.Set[String] = jedis.smembers(orderDetailRedisKey)
            //将java集合转为Scala集合
            for (elem <- detailsJsonStr.asScala) {
              //将查询出来的json字符串的orderDetail数据转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            }
          }
        } else {
          //orderInfo不在
          //todo 判断orderDetail是否存在
          if (detailOpt.isDefined) {
            val orderDetail: OrderDetail = detailOpt.get
            //去对方缓存中查询是否有对应的order Info数据
            //判断orderInfo的rediskey是否存在
            if (jedis.exists(orderDetailRedisKey)) {
              //有数据,取出来
              val infoJsonStr: String = jedis.get(orderInfoRedisKey)
              //将查询中的字符串转为样例类
              val orderInfo: OrderInfo = JSON.parseObject(infoJsonStr, classOf[OrderInfo])
              //和orderDetail一起组合为SaleDetail样例类
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            } else {
              //todo 对方缓存没有数据可以关联,则将自己写入缓存(redis)
              //将样例类转为JSON字符串
              val detailJsonStr: String = Serialization.write(orderDetail)
              jedis.sadd(orderDetailRedisKey, detailJsonStr)
              //对其设置过期时间
              jedis.expire(orderDetailRedisKey, 20)

            }
          }
        }

      }
      //在每一个分区下关闭redis连接
      jedis.close()
      //将结果集合转为Scala集合并转为迭代器返回
      details.asScala.toIterator
    })
    //打印测试双流join+缓存的方式是否解决了因网络延迟所带来的数据丢失问题
    noUserSaleDetailDStream.print()

    //7.反查缓存,关联user Info数据
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(partition => {
      //创建redis连接
      val jedis: Jedis = new Jedis("mospade202", 6379)

      partition.map(saleDetail => {

        //1.取用户表的数据
        val userInfoRedisKey: String = "userInfo" + saleDetail.user_id

        val userInfoJsonStr: String = jedis.get(userInfoRedisKey)

        //2.将数据转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])

        //3.关联userInfo数据
        saleDetail.mergeUserInfo(userInfo)

        saleDetail
      })
      jedis.close()
      partition
    })
    saleDetailDStream.print()

    //8.将关联的明细数据写入ES
    saleDetailDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(partition =>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          //将订单明细的id做为es的docid,因为它不会重复,粒度更小
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_SALEDETAIL_PREFIX+"0726",list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
