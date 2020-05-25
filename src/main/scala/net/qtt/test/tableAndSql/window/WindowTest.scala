package net.qtt.test.tableAndSql.window

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, GroupWindow, Slide, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
/**
 * @Description
 * @Author sunkl 
 * @date 2020/5/5 5:42 下午
 * @version 0.0.1
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    flinkEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    flinkEnv.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build()
    val tableEnv = StreamTableEnvironment.create(flinkEnv, settings)
    val stream = flinkEnv.socketTextStream("localhost", 19999)
      .flatMap(line=>line.split(",| ").map(w=>(w,1,new Date().getTime)))
    //引入水位线，添加数据延迟支持，由于table无法不支持水位线，所以在stream阶段添加水位线支持
      //引入watermark 让窗口延迟出发
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String,Int, Long)](Time.seconds(3)) {
        override def extractTimestamp(t: (String,Int, Long)): Long = {
          t._3
        }
      })
    val table= tableEnv.fromDataStream(stream,'word,'word_c,'update_time.rowtime)
    //滚动窗口 1
//    val result = table.window(Tumble.over("5.second").on("update_time").as("window"))
      val result: Table = table.window(Tumble over 5.second on 'update_time as 'window)
//      //必须使用两个字段分组，window和待分组字段
      .groupBy('window, 'word)
      .select('word, 'window.start, 'window.end,'word.count,'word_c.sum)
    tableEnv.toRetractStream[Row](result).print()
    tableEnv.execute("test")
//    //滚动窗口2
//    table.window(Tumble over 5.second on 'update_time as 'window)
//    //滑动窗口1
//    table.window(Slide over 10.second  every 5.second on 'update_time as 'window)
//    //滑动窗口2
//    table.window(Slide.over("5.second").every("5.second").on('update_time).as("window"))


  }
}

class MyFlateMap extends TableFunction[Row]{
  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING,Types.INT,Types.LONG)
  }
  def eval(str:String,timeStamp:Long)={
    str.split(",| ").foreach(w=>{
      val row = new Row(3)
      row.setField(0,w)
      row.setField(1,1)
      row.setField(2,timeStamp)
      collect(row)
    })
  }
}