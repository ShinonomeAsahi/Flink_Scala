package Module_D

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

object OnTimer {
  case class test(id: String,temp: Int)
  def main(args: Array[String]): Unit = {
    // 初始化流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)

    // 实例化一个Properties
    val prop = new Properties()
    // 设置bootstrapServers、zookeeperConnect、groupId
    prop.setProperty("bootstrap.servers","master:9092")
    prop.setProperty("zookeeper.connect", "master:2181,slave1:2181,slave2:2181")
    prop.setProperty("group.id", "test")

    // 实例化FlinkKafkaConsumer，传入topic、序列化方法、连接参数
    val consumer = new FlinkKafkaConsumer[String]("hunter", new SimpleStringSchema(), prop)
    //设置只读取最新数据
    consumer.setStartFromLatest()

    //添加kafka为数据源
    val stream = env.addSource(consumer)
//    val resultDataStream=stream.flatMap(_.split(" "))
//    .filter(_.nonEmpty)
//    .map((_,1))
//    .keyBy(0)
//    .sum(1)
    val mapStream=stream.map { x => 
     val arr=x.split(",")
     test(arr(1),arr(2).toInt)
    }

    // DataStream转KeyedStream，根据key做sum
    val resultDataStream=mapStream
      // 要使用case class，使用keyBy(_.id)的方式，不能使用tuple类型keyBy(0)的形式，否则窗口不知道id的类型（推测）
      .keyBy(_.id)
      .process(new TempIncreWarning(10))

    resultDataStream.print()
    
    // 对结果数据类型进行转换
    //val rx=resultDataStream.map(x =>(x._1,x._2.toString()))
    //rx.print()

    // 配置访问的Redis服务器地址和端口（6379）
    //val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()

    // 写入redis，传入redis配置参数和RedisMapper
    //rx.addSink(new RedisSink[(String,String)](conf,new MyRedisSink))

    env.execute("Flink 消费kafka数据写入Redis范例")
  }

  // 创建一个子类，继承自RedisMapper
  class MyRedisSink extends RedisMapper[Tuple2[String,String]] {

    // 重写getCommandDescription，设置RedisCommand
    override def getCommandDescription:RedisCommandDescription={

      //Redis Hset 命令用于为哈希表中的字段赋值 。访问时需要采用 hget hset名 key
      //Redis set 可以直接get key
      new RedisCommandDescription(RedisCommand.SET)
    }
    // 重写getKeyFromData，设置传入Redis的key
    override def getKeyFromData(data:(String,String)):String=data._1

    // 重写getValueFromData，设置传入Redis的value
    override def getValueFromData(data:(String,String)):String=data._2
  }

  // 实现自定义的KeyedProcessFunction
  class TempIncreWarning(inerval: Long) extends KeyedProcessFunction[String, test, String]{
    // 定义状态保存上一次温度值进行判断，保存注册定时器的时间戳用于删除
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))

    override def processElement(value: test, ctx: KeyedProcessFunction[String, test, String]#Context, collector: Collector[String]): Unit = {
      // 先取出状态
      val lastTemp = lastTempState.value()
      val timerTs = timerTsState.value()

      // 更新温度值
      lastTempState.update(value.temp)

      // 当前温度值是否大于39
      if( value.temp > 39 && timerTs == 0 ){
        // 如果温度值大于39，且没有定时器，那么注册当前数据时间戳10分钟之后的定时器
        val ts = ctx.timerService().currentProcessingTime() + inerval*60000
        ctx.timerService().registerProcessingTimeTimer(ts)
        timerTsState.update(ts)
      } else if( value.temp < 39 ){
        // 如果温度低于39度，那么删除定时器
        ctx.timerService().deleteProcessingTimeTimer(timerTs)
        timerTsState.clear()
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, test, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("温度连续10分钟高于39度！")
      timerTsState.clear()
    }
  }


  // 功能测试
//  class MyKeyedProcessFunction() extends KeyedProcessFunction[String, test, String]{
//
//    // 定义状态
//    var myState: ValueState[Int] = _
//
//    override def open(parameters: Configuration): Unit = {
//      myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("mystate",classOf[Int]))
//    }
//
//    override def processElement(i: (String, Int), context: KeyedProcessFunction[String, (String, Int), String]#Context, collector: Collector[String]): Unit = {
//      // 获取当前key
//      context.getCurrentKey
//      // 获取当前数据时间戳
//      context.timestamp()
//      // 获取当前EventTimebn
//      context.timerService().currentWatermark()
//      //注册一个一分钟定时器
//      context.timerService().registerEventTimeTimer(context.timestamp() + 60000L)
//
//    }

}