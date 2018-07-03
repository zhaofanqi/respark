import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object wordcount {
  def main(args: Array[String]): Unit = {
    //获取StreamingContext对象，并设定微批次处理时间间隔
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    //创建一个接收器来接受数据DStream[String]
    val line = ssc.socketTextStream("hadoop102", 9999)

    val words = line.flatMap((_).split(" "))

    val kv = words.map((_, 1))

    //将相同单词个数进行合并
    val ks = kv.reduceByKey(_ + _)

    //输出结果
    ks.print()
    ssc.start()

    //异常退出时可以捕获错误用
    ssc.awaitTermination()
  }
}
