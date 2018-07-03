package com.atguigu


import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 功能：从kafka中读取数据并写入另一个kafka
  *
  * @author zhaofanqi
  *
  */
object StreamingKafka {
  def main(args: Array[String]): Unit = {
    //创建连接
    val sparkconf = new SparkConf().setAppName("stream").setMaster("local[*]")
    val ssc = new StreamingContext(sparkconf, Seconds(5))

    //连接kafka
    //连接参数设定

    //创建连接kafka的参数
    val brokerList = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val zookeeper = "hadoop102:2181,hadoop103:2181,hadoop104:2181"
    val sourceTopic = "source2"
    val targetTopic = "target2"
    val groupid = "atguigu"
    //创建连接kafka的参数
    val kafkaParam = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )
    //读取数据
    val textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(sourceTopic))
    //数据写出
    textKafkaDStream.map(s => "key: " + s._1 + "value: " + s._2).foreachRDD { rdd =>

      rdd.foreachPartition { items =>
        //写回kafka
        val pool = KafkaPool(brokerList)
        val kafkaProxyConsumer = pool.borrowObject()
        //创建到kafka连接，使用自定义工具类
        for (item <- items) {
          println(items)

          kafkaProxyConsumer.send(targetTopic, item)
          //返回连接
          pool.returnObject(kafkaProxyConsumer)
        }
        //写数据
        //关闭连接
      }
    }

    //连接开启后一直占用
    ssc.start()
    println("ssc.start()")
    ssc.awaitTermination()
  }
}
