package cn.michael.spark.ml.feature

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by hufenggang on 2021/7/14.
 */
object QuantileDiscretizerExample {
  private final val JOB_NAME: String = "QuantileDiscretizerExample"

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

    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = spark.createDataFrame(data).toDF("id", "hour")

    val discretizer: QuantileDiscretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("quantile_discretizer_feature")
      .setNumBuckets(3)

    val result: DataFrame = discretizer.fit(df).transform(df)
    result.show()

    spark.stop()
  }

}
