package net.qtt.test.tableAndSql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
/**
 * @Description
 * @Author sunkl 
 * @date 2020/5/4 11:35 下午
 * @version 0.0.1
 */
object TableAndSqlNetCat {
  def main(args: Array[String]): Unit = {
    val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val environmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(flinkEnv, environmentSettings)
    val stream = flinkEnv.socketTextStream("localhost", 19999).map(line => {
      val arr = line.split(",")
      BeanClass(arr.head, arr(1).toInt, arr.last)
    })
//    val tableStream = tableEnv.registerDataStream("t_stream", stream)
//    val table = tableEnv.scan("t_stream")
//    table.printSchema()
//    tableEnv.sqlQuery("select * from t_stream").printSchema()
//    val table = tableEnv.fromDataStream(stream, 'name1,'sexd,'dfs)
//    table.printSchema()
    //  过滤
//    val table =tableEnv.fromDataStream(stream)
//    val filterTable = table.filter('name==="sunkl").select('name,'age,'sex)
//    val ds = tableEnv.toRetractStream[Row](filterTable)
//    ds.print()
    //分组聚合
//    val table = tableEnv.fromDataStream(stream).groupBy('name).select('name,'sex.count,'age.sum)
//    tableEnv.toAppendStream[Row](table).print()
//    flinkEnv.execute()
  }
}
case class BeanClass(name:String,age:Int,sex:String)