package cn.michael.spark.example.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hufenggang on 2021/6/1.
 */
object SparkShuffleExample1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("DataSkewCase1")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    val list1 = List(
      "A B C D E",
      "A B C D",
      "A B C E",
      "A B D E",
      "A C D E",
      "A C D E",
      "A B C D"
    )

    val list2 = List(
      "A B C D E",
      "A B C D",
      "A B C E",
      "A B D E",
      "A C D E",
      "A C D E",
      "A B C D"
    )

    val value1: RDD[(String, Int)] = sc.parallelize(list1)
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
  }

}
