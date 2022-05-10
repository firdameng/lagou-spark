package org.example.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    // hdfs读取文件
    //val lines: RDD[String] = sc.textFile("hdfs://hadoop1:9000/wcinput/wc.txt")
    // hdfs读取文件，有配置文件core-site.xml
    val lines: RDD[String] = sc.textFile("/wcinput/wc.txt")
    lines.partitioner
    // 本地读取文件
    //val lines: RDD[String] = sc.textFile("data/wc.txt")
    // val lines: RDD[String] = sc.textFile("data/wc.dat")
    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
    // 暂停程序查看web界面
    Thread.sleep(1000000)
    sc.stop()
  }
}
