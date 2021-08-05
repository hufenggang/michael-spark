package cn.michael.spark.ml.feature.extract

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by hufenggang on 2021/7/19.
 */
object TDIDFExample {
  private final val JOB_NAME: String = "TDIDFExample"

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
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")

    val wordsData: DataFrame = tokenizer.transform(dataFrame)

    val hashingTF: HashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)

    val featurizedData: DataFrame = hashingTF.transform(wordsData)

  }

}
