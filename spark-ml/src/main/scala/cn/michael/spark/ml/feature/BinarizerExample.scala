package cn.michael.spark.ml.feature

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by hufenggang on 2021/7/13.
 *
 * 连续性数据处理之二值化
 */
object BinarizerExample {
  private final val JOB_NAME: String = "BinarizerExample"

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

    val data = Array((0, 0.1), (1, 0.8), (2, 0.6))
    val dataFrame: DataFrame = spark.createDataFrame(data)
      .toDF("id", "feature")

    // transform 开始转换,将该列数据二值化，大于阈值的为1.0，否则为0.0
    val binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame : DataFrame = binarizer.transform(dataFrame)

    binarizedDataFrame.show()

    spark.stop()
  }

}
