package cn.michael.spark.ml.feature

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by hufenggang on 2021/7/14.
 */
object BucketizerExample {
  private final val JOB_NAME: String = "BucketizerExample"

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.localShuffleReader", "true")
      .set("spark.sql.adaptive.skewedJoin.enabled", "true")
      .set("spark.sql.adaptive.join.enabled", "true")
      .set("spark.testing.memory","2147480000")
      .setAppName(JOB_NAME)

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val data = Array((1,13.0),(2,16.0),(3,23.0),(4,35.0),(5,56.0),(6,44.0))

    // 将数组转化为DataFrame
    val dataFrame: DataFrame = spark.createDataFrame(data).toDF("id", "age")

    // 设定边界，分为5个年龄组：[0,20),[20,30),[30,40),[40,50),[50,正无穷)
    // 注：人的年龄当然不可能正无穷，为了演示正无穷PositiveInfinity的用法，负无穷是NegativeInfinity。
    val splits = Array(0, 20, 30, 40, 50, Double.PositiveInfinity)

    // 初始化Bucketizer对象并进行设定：setSplits是设置我们的划分依据
    val bucketizer: Bucketizer = new Bucketizer().setSplits(splits)
      .setInputCol("age")
      .setOutputCol("bucketizer_feature")

    val bucketizerDF: DataFrame = bucketizer.transform(dataFrame)
    bucketizerDF.show()

    spark.stop()
  }

}
