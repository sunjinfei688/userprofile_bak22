package cn.itcast.up.statistics

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame, functions}

/**
 * Author itcast
 * Date 2019/12/7 14:57
 * Desc 开发消费周期模型/标签
 *消费周期主要是获取用户在平台的最近一次消费时间,比如用户最近/后一次的消费时间是1周前还是1个月前.方便我们获取到长时间未消费的用户.
 */
object CycleModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  /**
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @return
   */
  override def getTagId(): Long = 23

  /**
   * 根据5级标签和HBase数据进行标签计算(每个模型/标签计算方式不同)
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @param fiveDF 5级标签
   * @param hbaseDF 根据4级标签查询出来的HBase数据
   * inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_orders##family=detail##selectFields=memberId,finishTime
   * @return
   */
  override def compute(fiveDF: DataFrame, hbaseDF: DataFrame): DataFrame = {
    //fiveDF.show(10,false)
    //fiveDF.printSchema()
    /*
+---+-----+
|id |rule |
+---+-----+
|24 |0-7  |
|25 |8-14 |
|26 |15-30|
|27 |31-60|
|28 |61-90|
+---+-----+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)
     */

    //hbaseDF.show(10,false)
    //hbaseDF.printSchema()
    /*
+---------+----------+
|memberId |finishTime|
+---------+----------+
|13823431 |1564415022|
|4035167  |1565687310|
|4035291  |1564681801|
|4035041  |1565799378|
|13823285 |1565062072|
|4034219  |1563601306|
|138230939|1565509622|
|4035083  |1565731851|
|138230935|1565382991|
|13823231 |1565677650|
+---------+----------+
only showing top 10 rows

root
 |-- memberId: string (nullable = true)
 |-- finishTime: string (nullable = true)
     */
    //1.获取用户最近一次消费时间
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val tempDF: DataFrame = hbaseDF.groupBy('memberId.as("userId"))
      .agg(max('finishTime).as("maxfinishTime"))
    //tempDF.show(10,false)
    //tempDF.printSchema()
    /*
 +---------+-------------+
|userId   |maxfinishTime|
+---------+-------------+
|13822725 |1566056954   |
|13823083 |1566048648   |
|138230919|1566012606   |
|13823681 |1566012541   |
|4033473  |1566022264   |
|13822841 |1566016980   |
|13823153 |1566057403   |
|13823431 |1566034616   |
|4033348  |1566030627   |
|4033483  |1566048851   |
+---------+-------------+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- maxfinishTime: string (nullable = true)
     */

    //2.计算最近一次消费时间距离今天的天数(今天的时间戳-最近一次消费时间戳)/(每天毫秒数:24*60*60*1000)
    //current_date()获取当前日期
    //date_sub(current_date(),30)将当前日期往前推30天
    //from_unixtime(long值)将时间戳/字符串转成日期
    //datediff(start,end)计算两个日期的时间差(天数)
    val timediffColume: Column = functions.datediff(date_sub(current_date(),30),from_unixtime('maxfinishTime.cast(LongType)))
    val tempDF2: DataFrame = tempDF.select('userId,timediffColume.as("days"))
    tempDF2.show(10,false)
    tempDF2.printSchema()
    /*
 +---------+----+
|userId   |days|
+---------+----+
|13822725 |82  |
|13823083 |82  |
|138230919|82  |
|13823681 |82  |
|4033473  |82  |
|13822841 |82  |
|13823153 |82  |
|13823431 |82  |
|4033348  |82  |
|4033483  |82  |
+---------+----+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- days: integer (nullable = true)

     */

    //3.将fiveDF中的rule列拆成start和end
    val rangDF: DataFrame = fiveDF.map(row => {
      val id: Long = row.getAs[Long]("id")
      val rule: String = row.getAs[String]("rule")
      val arr: Array[String] = rule.split("-")
      (id, arr(0).toInt, arr(1).toInt)
    }).toDF("tagsId", "start", "end")

    //4.将tempDF2和rangDF进行匹配
    val newDF: DataFrame = tempDF2.join(rangDF)
      .where(tempDF2.col("days").between(rangDF.col("start"),'end))
      .select('userId, 'tagsId)
    newDF.show(10,false)
    /*
 +---------+------+
|userId   |tagsId|
+---------+------+
|13822725 |28    |
|13823083 |28    |
|138230919|28    |
|13823681 |28    |
|4033473  |28    |
|13822841 |28    |
|13823153 |28    |
|13823431 |28    |
|4033348  |28    |
|4033483  |28    |
+---------+------+
     */

    newDF
  }
}
