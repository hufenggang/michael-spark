package cn.michael.spark.ml.basic.statistics

import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by hufenggang on 2021/8/2.
 */
object CorrelationExample {
  private final val JOB_NAME: String = "CorrelationExample"

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

    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.,0),
      Vectors.dense(5.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    import spark.implicits._

    val df = data.map(Tuple1.apply).toDF("features")

    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head()
    println(s"Pearson correlation matrix:\n $coeff1")

    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head()
    println(s"Pearson correlation matrix:\n $coeff1")
  }

}
