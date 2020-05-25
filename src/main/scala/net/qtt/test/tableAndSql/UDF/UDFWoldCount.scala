package net.qtt.test.tableAndSql.UDF
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
object UDFWoldCount {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(environment, settings)
    val stream = environment.socketTextStream("localhost", 19999)
//      .map(line => line.split(","))
//      .filter(arr => arr.length == 3)
//        .map(arr=>(arr.head,arr(1),arr.last))
    val my_func = new MyFlatMapFunction
    val table= tableEnv.fromDataStream(stream,'line)
        .flatMap(my_func('line)).as('word,'word_c)
        .groupBy('word)
        .select('word,'word_c.sum as 'c)
    tableEnv.toRetractStream[Row](table).print()

    environment.execute()
  }
}
class MyFlatMapFunction extends TableFunction[Row]{
  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING,Types.INT)
  }
  def eval(str:String)={
    str.trim.split(",").foreach(f=>{
      val row = new Row(2)
      row.setField(0,f)
      row.setField(1,1)
      collect(row)
    })
  }
}