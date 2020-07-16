package ml.test

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

/**
 * @Description
 * @Author sunkl 
 * @date 2020/7/15 11:45 上午
 * @version 0.0.1
 */
object LogisticRegression {
  lazy val spark = SparkSession.builder().appName("est").master("local").getOrCreate()

  def exeLogisticRegression(): Unit = {
    val data = spark.read.format("libsvm").load("file:///Users/admin/IdeaProjects/net.qtt/flinkTest/src/main/config/test.libsvm")
    val lr = new LogisticRegression()
      .setElasticNetParam(0.8)
      .setMaxIter(8)
      .setRegParam(0.05)
      .setFitIntercept(false)
    val model = lr.fit(data)
    val summary = model.binarySummary
    val objectHIs =summary.objectiveHistory.mkString(",")
    println(objectHIs)
    model.binarySummary.roc.show()
    model.binarySummary
    println(summary.areaUnderROC)
  }

  def main(args: Array[String]): Unit = {
    exeLogisticRegression()
  }
}
