package Module_D

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object Flink_0526 {

  case class order_info(detail_id: String, order_id: String, sku_id: String, sku_name: String, order_price: Double, sku_num: String, sources_type: String, sources_id: String,
                     id: String, consignee: String, consignee_tel: String, final_total_amount: Double, order_status: String, user_id: String, delivery_address: String,
                     order_command: String, out_trade_no: String, trade_body: String, create_time: Long, operate_time: Long, expire_time: Long, tracking_no: String,
                     parent_order_id: String, img_url: String, province_id: String, benefit_reduce_amount: Double, original_total_amount: Double, feight_fee: Double)

  case class topN(user_id: String, user_total: Double, window_end: Long)

  // 将String类型时间解析为时间戳
  def timeParse( time: String): Long ={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(time).getTime
  }

  // 将时间戳格式化为String
  def time2String( time: String): String ={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(time)
  }

  // 定义侧输出流标签
  private val cancelStream : OutputTag[order_info] = OutputTag[order_info]("cancel")

  def main(args: Array[String]): Unit = {

    // 实例化流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度为1
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 设置kafka集群信息
    val props = new Properties()
    props.setProperty("bootstrap.servers", "192.168.43.111:9092")
    props.setProperty("gruop.id", "test")

     //连接kafka
     val consumer = new FlinkKafkaConsumer("order", new SimpleStringSchema(), props)
    //设置只读最新数据
     consumer.setStartFromLatest()

     //把Kafka作为数据源添加进env并指定EventTime
     val stream = env.addSource(consumer).map { x =>
       val arr = x.split(",")
       order_info(arr(0),arr(1),arr(2),arr(3),arr(4).toDouble,arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11).toDouble,arr(12),arr(13),arr(14),arr(15),arr(16),arr(17),timeParse(arr(18)),timeParse(arr(19)),timeParse(arr(20)),arr(21),arr(22),arr(23),arr(24),arr(25).toDouble,arr(26).toDouble,arr(27).toDouble)
     }
       .assignTimestampsAndWatermarks(
       new BoundedOutOfOrdernessTimestampExtractor[order_info](Time.seconds(5)) {
         override def extractTimestamp(t: order_info) = {
           val create = t.create_time
           var operate = t.operate_time
           if (operate.equals("")){
             operate = create
           }
           create.max(operate)
         }
       }
     )

    // 获取购买量TopN的User
    val topNuser = stream
      .keyBy(_.user_id)
      .timeWindow(Time.minutes(10),Time.seconds(60))
      .aggregate(new agg(), new myWindow())
      .keyBy(_.window_end)
      .process(new myTopN())

    // 获取侧输出流
    val split = stream
      .process(new ProcessFunction[order_info,order_info]{
        override def processElement(i: order_info, context: ProcessFunction[order_info, order_info]#Context, collector: Collector[order_info]) = {
          context.output(cancelStream,i)
        }
      })

    // 计算取消订单占所有订单的比值
    split.getSideOutput(cancelStream)
      .keyBy(_.detail_id)
      .timeWindow(Time.seconds(1))
      .process(new ProcessWindowFunction[order_info,String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[order_info], out: Collector[String]): Unit = {
          val List1005 = new ListBuffer[order_info]
          if (elements.iterator.next().order_status.equals("1005")) {
            List1005 += elements.iterator.next()
          }
          out.collect(((List1005.size.toDouble / elements.iterator.size.toDouble)*100).formatted("%.1f") + "%")
        }
      }).print()

    val pool = new FlinkJedisPoolConfig.Builder().setHost("192.168.43.111").setPort(6379).build()
    val RedisSink1 = new RedisSink[String](pool,new myRedisMapper1)

    topNuser.addSink(RedisSink1)
    //topNuser.print()

    env.execute("Flink_0526")
  }

  // 定义累加器，获取订单总额
  class agg() extends AggregateFunction[order_info,Double,Double]{

    override def createAccumulator(): Double = 0.0

    override def add(in: order_info, acc: Double): Double = acc + in.order_price

    override def getResult(acc: Double): Double = acc

    override def merge(acc: Double, acc1: Double): Double = acc + acc1
  }

  // 使用ProcessWindowFunction获取Key，各用户累计订单金额，窗口结束时间
  class myWindow() extends ProcessWindowFunction[Double,topN,String,TimeWindow]{

    override def process(key: String, context: Context, elements: Iterable[Double], out: Collector[topN]): Unit = {
      out.collect(topN(key,elements.iterator.next(),context.window.getEnd))
    }
  }

  // 使用KeyedProcessFunction获取TopN
  class myTopN() extends KeyedProcessFunction[Long,topN,String]{

    // 初始化ListState，保存每个window中的数据
    private var topNListState: ListState[topN] = _
    // 初始化数据库Connection
    var conn:Connection = _

    // 重写open生命周期
    override def open(parameters: Configuration): Unit = {
      // 定义ListStateDescriptor
      topNListState = getRuntimeContext.getListState(new ListStateDescriptor[topN]("TopNList",classOf[topN]))
      // 连接数据库
      conn = DriverManager.getConnection("jdbc:mysql://192.168.43.111/train_store?useSSL=false","root","123456")
    }

    override def processElement(i: topN, context: KeyedProcessFunction[Long, topN, String]#Context, collector: Collector[String]): Unit = {
      // 将窗口每个新元素存入ListState中
      topNListState.add(i)
      // 注册一个窗口结束时间+1的定时器
      context.timerService().registerEventTimeTimer(i.window_end + 1)
    }

    // 重写onTimer，指定定时器被触发时的操作
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, topN, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 初始化一个ListBuffer
      val allPrice = new ListBuffer[topN]
      // topNListState是Java类型？定义隐式转换
      import scala.collection.JavaConversions._
      // 循环遍历ListState，将所有数据保存到ListBuffer中
      for ( i <- topNListState.get()){
        allPrice += i
      }

      // 在ListBuffer中根据_.user_total排序，使用函数柯里化进行降序并取出前2条
      val sorted = allPrice.sortBy(_.user_total)(Ordering.Double.reverse).take(2)

      // 清空ListState以接收下一次数据
      topNListState.clear()

      // 建立一个StringBuilder进行格式化输出
      val stringBuilder = new StringBuilder
      // 根据ListBuffer索引进行遍历
      for( i <- sorted.indices){

        // 定义数据库查询语句
        val preparedStatement = conn.prepareStatement("select name from user_info where id = ?")
        // 指定占位符
        preparedStatement.setString(1,sorted(i).user_id)
        // 将查询结果保存到变量中
        val userName = preparedStatement.executeQuery()
        // 初始化name
        var name = "null"
        // 对查询结果循环取值保存到name中
        while (userName.next()) {
          name = userName.getString(1)
        }
        if(i == 0){
          stringBuilder.append("[")
          stringBuilder.append(sorted(i).user_id.mkString)
          stringBuilder.append(":")
          stringBuilder.append(name.mkString)
          stringBuilder.append(":")
          stringBuilder.append(sorted(i).user_total.toString)
        } else {
          stringBuilder.append(",")
          stringBuilder.append(sorted(i).user_id.mkString)
          stringBuilder.append(":")
          stringBuilder.append(name.mkString)
          stringBuilder.append(":")
          stringBuilder.append(sorted(i).user_total.toString)
          stringBuilder.append("]")
        }

        // 关闭查询状态
        preparedStatement.close()
      }
      // 将StringBuilder放入out收集器中
      out.collect(stringBuilder.toString())
    }

    // 重写close生命周期，关闭数据库连接
    override def close(): Unit = {
      super.close()
      conn.close()
    }
  }

  // 重写RedisMapper，将数据写入Redis
  class myRedisMapper1 extends RedisMapper[String]{
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.SET)
    }

    override def getKeyFromData(t: String): String = {
      "total2userconsumption"
    }

    override def getValueFromData(t: String): String = {
      t
    }
  }


}

