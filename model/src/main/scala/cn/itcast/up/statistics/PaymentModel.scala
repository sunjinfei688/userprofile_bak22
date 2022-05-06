package cn.itcast.up.statistics

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * Author itcast
 * Date 2019/12/7 16:26
 * Desc 完成支付方式标签的开发
 * 用户的支付方式有多种,比如使用支付宝/微信/货到付款/银联等等.我们需要知道用户最常用的支付方式,以便于了解用户最常用的支付平台.
 * 我们要去统计用户最常用的支付方式
 * 其实做分组TopN
 */
object PaymentModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }
  /**
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @return
   */
  override def getTagId(): Long = 29

  /**
   * 根据5级标签和HBase数据进行标签计算(每个模型/标签计算方式不同)
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_orders##family=detail##selectFields=memberId,paymentCode
   * @param fiveDF
   * @param hbaseDF
   * @return
   */
  override def compute(fiveDF: DataFrame, hbaseDF: DataFrame): DataFrame = {
    //fiveDF.show(10,false)
    //fiveDF.printSchema()
    //hbaseDF.show(10,false)
    //hbaseDF.printSchema()
    /*
 +---+--------+
|id |rule    |
+---+--------+
|30 |alipay  |
|31 |wxpay   |
|32 |chinapay|
|33 |kjtpay  |
|34 |cod     |
|35 |other   |
+---+--------+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)

+---------+-----------+
|memberId |paymentCode|
+---------+-----------+
|13823431 |alipay     |
|4035167  |alipay     |
|4035291  |alipay     |
|4035041  |alipay     |
|13823285 |kjtpay     |
|4034219  |alipay     |
|138230939|alipay     |
|4035083  |alipay     |
|138230935|alipay     |
|13823231 |cod        |
+---------+-----------+
only showing top 10 rows

root
 |-- memberId: string (nullable = true)
 |-- paymentCode: string (nullable = true)
     */

    import org.apache.spark.sql.functions._
    import spark.implicits._

    //1.按照memberId和paymentCode进行分组计数
    val tempDF: DataFrame = hbaseDF.groupBy('memberId, 'paymentCode)
      .agg(count('paymentCode).as("counts"))
      .select('memberId.as("userId"),'paymentCode,'counts)

    //tempDF.show(10,false)
    //tempDF.printSchema()
    /*
 +--------+-----------+------+
|userId  |paymentCode|counts|
+--------+-----------+------+
|13823481|alipay     |96    |
|4035297 |alipay     |80    |
|13823317|kjtpay     |11    |
|13822857|alipay     |100   |
|4034541 |alipay     |96    |
|4034209 |cod        |17    |
|4034863 |alipay     |90    |
|4033371 |alipay     |95    |
|13822723|alipay     |148   |
|4034193 |cod        |16    |
+--------+-----------+------+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- paymentCode: string (nullable = true)
 |-- counts: long (nullable = false)
     */

    //2.求分组top1
    val rankDF: Dataset[Row] = tempDF
      //withColumn(列名,列如何计算)表示增加一列
      .withColumn("rn", row_number().over(Window.partitionBy('userId, 'paymentCode).orderBy('counts.desc)))
    .where('rn === 1)
    //rankDF.show(10,false)
    //rankDF.printSchema()
    /*
 +---------+-----------+------+---+
|userId   |paymentCode|counts|rn |
+---------+-----------+------+---+
|13822721 |wspay      |5     |1  |
|13822723 |alipay     |148   |1  |
|13822731 |kjtpay     |10    |1  |
|13822819 |wspay      |5     |1  |
|13822857 |alipay     |100   |1  |
|13823039 |chinapay   |1     |1  |
|13823067 |giftCard   |1     |1  |
|13823077 |prepaid    |1     |1  |
|138230811|alipay     |2     |1  |
|138230951|wspay      |4     |1  |
+---------+-----------+------+---+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- paymentCode: string (nullable = true)
 |-- counts: long (nullable = false)
 |-- rn: integer (nullable = true)
     */

    //3.将rankDF中的paymentCode替换成tagsId
    //Map[paymentCode, tagsId]
    val ruleMap: Map[String, Long] = fiveDF.as[(Long, String)].map(t => {
      (t._2, t._1)
    }).collect().toMap

    //baitiao
    val paymentCode2tag = udf((paymentCode:String)=>{
      //将paymentCode转为tagid并返回
      var tagsId: Long = ruleMap.getOrElse(paymentCode,-1)
      if (tagsId == -1){
        tagsId =  ruleMap("other")
      }
      tagsId
    })

    val newDF: DataFrame = rankDF.select('userId,paymentCode2tag('paymentCode).as("tagsId"))
    newDF.show(10,false)
    newDF.printSchema()

    /*
 +---------+-----+
|userId   |tagsId|
+---------+------+
|13822721 |35    |
|13822723 |30    |
|13822731 |33    |
|13822819 |35    |
|13822857 |30    |
|13823039 |32    |
|13823067 |35    |
|13823077 |35    |
|138230811|30    |
|138230951|35    |
+---------+------+
     */

    //newDF
    null
  }
}
