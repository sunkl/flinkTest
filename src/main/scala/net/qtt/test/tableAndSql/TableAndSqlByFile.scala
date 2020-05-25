package net.qtt.test.tableAndSql

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

/**
 * @Description
 * @Author sunkl 
 * @date 2020/5/4 12:40 下午
 * @version 0.0.1
 */
object TableAndSqlByFile {

  def main(args: Array[String]): Unit = {
    //创建flink environment
    val flineEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnvironment=StreamTableEnvironment.create(flineEnv,settings)

    val csvTableSource = new CsvTableSource("",
      Array("f1","f2","f3"),
      Array(Types.STRING,Types.LONG,Types.INT)
    )
    tableEnvironment.registerTableSource("test",csvTableSource)
    val table = tableEnvironment.scan("test")
    table.printSchema()

  }
}
