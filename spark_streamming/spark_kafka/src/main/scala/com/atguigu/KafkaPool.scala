package com.atguigu

import java.util.Properties

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


/**
  * 自定义类包装参数
  */
class  KafkaProxy(broker:String){

  private val  pros:Properties=new Properties()
  pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker)
  pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
  pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

  private val kafkaConn=new KafkaProducer[String,String](pros)

 def send(topic:String,key:String,value:String): Unit ={
   kafkaConn.send(new ProducerRecord[String,String](topic,key,value))
 }

 def send(topic:String,value:String): Unit ={
   kafkaConn.send(new ProducerRecord[String,String](topic,value))
 }

  def close(): Unit ={
    kafkaConn.close()
  }

}

class KafkaProxyFactory(broker:String) extends BasePooledObjectFactory[KafkaProxy]{

  override def create(): KafkaProxy = new KafkaProxy(broker)

  override def wrap(t: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](t)
}



object KafkaPool {

/*private var kafkaProxyPool:GenericObjectPool[KafkaProxy]=null
  def apply(brokers:String ): GenericObjectPool[KafkaProxy]={
    if (null==kafkaProxyPool){
      KafkaPool.synchronized{
        if (null==kafkaProxyPool){
          kafkaProxyPool=new GenericObjectPool[KafkaProxy](new  KafkaProxyFactory(brokers))
        }
      }
    }
    kafkaProxyPool
  }*/
//声明一个连接池对象
private var kafkaProxyPool: GenericObjectPool[KafkaProxy] = null

  def apply(brokers:String) : GenericObjectPool[KafkaProxy] = {
    if(null == kafkaProxyPool){
      KafkaPool.synchronized{

        if(null == kafkaProxyPool){
          kafkaProxyPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(brokers))
        }

      }
    }
    kafkaProxyPool
  }
}
