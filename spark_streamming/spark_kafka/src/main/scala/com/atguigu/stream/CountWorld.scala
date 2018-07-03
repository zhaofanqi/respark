package com.atguigu.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CountWorld {


  var updateFunc = (values: Seq[Int], state: Option[Int]) => {
    var previousState = state.getOrElse(0)
    Some(previousState + values.sum)
  }

  def main(args: Array[String]): Unit = {
    //创建sparkConf对象
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    //创建StreamingConext对象
    val ssc = new StreamingContext(conf, Seconds(5))

    //创建一个接收器来接受数据DStream[String]
    val lineDStream = ssc.socketTextStream("hadoop102", 9999)

    //需要有状态，必须设置CheckPoint目录
    ssc.checkpoint("./checkpoint")

    //flatMap转换 成为单词DStream[String]
    val words = lineDStream.flatMap(_.split(" "))

    //将单词转换为KV结构DStream[(String,1)]
    val KVDStream = words.map((_, 1))

    //将相同单词数进行合并
    /* val wordcount= KVDStream.reduceByKey(_+_)
    wordcount.print()*/

    //定义状态更新函数
   // val result = KVDStream.updateStateByKey[Int](updateFunc)
    //
 // val result= KVDStream.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,(a:Int,b:Int)=>a-b,Seconds(20),Seconds(10))
   val result1=  KVDStream.countByWindow(Seconds(20),Seconds(10))
   val result2=  KVDStream.countByValueAndWindow(Seconds(20),Seconds(10))

    result1.print()
    println("*********************")
    result2.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
