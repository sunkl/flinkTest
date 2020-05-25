package net.qtt.test.tableAndSql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * @Description
 * @Author sunkl 
 * @date 2020/5/4 12:28 下午
 * @version 0.0.1
 */
object TableAndSqlTes {
  def main(args: Array[String]): Unit = {
    //创建flink environment
    val flineEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnvironment=StreamTableEnvironment.create(flineEnv,settings)

  }
}
