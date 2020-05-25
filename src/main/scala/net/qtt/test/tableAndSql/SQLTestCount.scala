package net.qtt.test.tableAndSql

import java.util
import java.util.Date

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * @Description
 * @Author sunkl 
 * @date 2020/5/6 12:20 上午
 * @version 0.0.1
 */
object SQLTestCount {
  def main(args: Array[String]): Unit = {
    val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment
    flinkEnv.setParallelism(1)
    flinkEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    var tableEnv = StreamTableEnvironment.create(flinkEnv,setting)
    val stream = flinkEnv.socketTextStream("localhost", 19999)
        .map(line=>{
          val arr = line.split(",| ")
          (arr.head,arr(1).toInt,arr.last,new Date().getTime)
        })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int, String,Long)](Time.seconds(3)) {
        override def extractTimestamp(t: (String, Int, String,Long)): Long = t._4
      })
    tableEnv.registerDataStream("test",stream,'name,'age,'sex,'u_time.rowtime)
//    val result = tableEnv.sqlQuery(
//      """
//        |select name,count(age),sum(age)
//        |from test
//        |where sex='boy'
//        |group by tumble(u_time,interval '25' second),name
//        |""".stripMargin)
    //如果提取窗口起始时间和结束时间，获取函数，必须放在聚合函数之前
val result = tableEnv.sqlQuery(
  """
    |select name,count(1),hop_start(u_time,interval '5' second,interval '10' second),hop_end(u_time,interval '5' second,interval '10' second),
    |sum(age)
    |from test
    |group by hop(u_time,interval '5' second,interval '10' second),name
    |""".stripMargin)
//    val table = tableEnv.fromDataStream(stream, 'name, 'age, 'sex,'u_time.rowtime)
//    tableEnv.registerTable("test",table)
//    val result = tableEnv.sqlQuery(
//      s"""
//        |select name,sum(age),count(age) from $table where sex='boy' group by name
//        |""".stripMargin)
    result.toRetractStream[Row].print()
    tableEnv.execute("test")
  }
}
