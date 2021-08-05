package cn.michael.spark.ml.feature.transform

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by hufenggang on 2021/7/19.
 */
object OneHotEncoderExample {
  private final val JOB_NAME: String = "OneHotEncoderExample"

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

    val dataFrame: DataFrame = spark.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0)
    )).toDF("index1", "index2")

    val encoder = new OneHotEncoder()
      .setInputCols(Array("index1", "index2"))
      .setOutputCols(Array("index_vec1", "index_vec2"))

    val model: OneHotEncoderModel = encoder.fit(dataFrame)
    val result: DataFrame = model.transform(dataFrame)

    result.show(20)
    spark.stop()
  }

}
