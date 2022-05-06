package cn.itcast.up.matchtag

import java.util
import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author itcast
 * Date 2019/12/5 10:16
 * Desc 性别标签(模型/任务)计算
 */
object GenderModel {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("GenderModel")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //导入隐式转换
    import spark.implicits._

    //1.读取MySQL数据--MySQLDF
    val url:String = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName:String = "tbl_basic_tag"
    val properties:Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url,tableName,properties)
    //mysqlDF.show(10,false)//false表示列很长的时候全部显示,不截断

    //2.读取MySQLDF中的4级标签的rule
    val fourRuleDS: Dataset[Row] = mysqlDF.select("rule").where("id=4")
    //fourRuleDS.show(10,false)
    //val fourRuleDS2: Dataset[Row] = mysqlDF.select($"rule").where($"id"===4)
    //fourRuleDS2.show(10,false)
    //val fourRuleDS3: Dataset[Row] = mysqlDF.select('rule).where('id ===4)
    //fourRuleDS3.show(10,false)
    /*
+-------------------------------------------------------------------------------------------------------------+
|rule                                                                                                         |
+-------------------------------------------------------------------------------------------------------------+
|inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,gender|
+-------------------------------------------------------------------------------------------------------------+
     */
    //3.解析rule,根据解析出来的信息,去读取HBase数据--HBaseDF
    val tempDS: Dataset[Array[(String, String)]] = fourRuleDS.map(row => {
      val ruleStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = ruleStr.split("##")
      kvs.map(kv => {
        val tempkvs: Array[String] = kv.split("=")
        (tempkvs(0), tempkvs(1))
      })
    })
    val tempList: util.List[Array[(String, String)]] = tempDS.collectAsList()
    val tempArray: Array[(String, String)] = tempList.get(0)
    val fourRuleMap: Map[String, String] = tempArray.toMap
    //fourRuleMap.foreach(println)
    /*
  (selectFields=id,gender)
  (inType=HBase)
  (zkHosts=192.168.10.20)
  (zkPort=2181)
  (hbaseTable=tbl_users)
  (family=detail)
     */
    //将fourRuleMap转换为方便后面使用的HBaseMeta样例类
    val hbaseMeta: HBaseMeta = HBaseMeta(fourRuleMap)

    //4.读取HBase数据--使用封装好的HBase数据源来读取(有开源的,也可以自己写)
    val hbaseDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      //HBaseMeta.SELECTFIELDS ,获取HBaseMeta中的常量
      //hbaseMeta.selectFields, 获取hbaseMeta样例类对象中的字段值
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .load()
    //hbaseDF.show(10,false)
    /*
  +---+------+
  |id |gender|
  +---+------+
  |1  |2     |
  |10 |2     |
  |100|2     |
  |101|1     |
  |102|2     |
  |103|1     |
  |104|1     |
  |105|2     |
  |106|1     |
  |107|1     |
  +---+------+
     */


    //5.读取MySQLDF中的5级标签--FiveDF
    val fiveDF: Dataset[Row] = mysqlDF.select('id,'rule).where('pid === 4)
    //fiveDF.show(10,false)
    /*
    tagid,gender
    +---+----+
    |id |rule|
    +---+----+
    |5  |1   |
    |6  |2   |
    +---+----+
     */
    //Map[gender, tagid]
    val fiveMap: Map[String, Long] = fiveDF.as[(Long, String)].map(t => {
      (t._2, t._1)
    }).collect().toMap

    //定义DSL风格的udf
    import org.apache.spark.sql.functions._
    val gender2tag = udf((gender:String)=>{
      fiveMap(gender)
    })

    //6.将HBaseDF和FiveDF进行关联,得到结果--resultDF
    val resultDF: DataFrame = hbaseDF.select('id.as("userId"),gender2tag('gender).as("tagsId"))
    resultDF.show(10,false)
/*
+------+------+
|userId|tagsId|
+------+------+
|1     |6     |
|10    |6     |
|100   |6     |
|101   |5     |
|102   |6     |
|103   |5     |
|104   |5     |
|105   |6     |
|106   |5     |
|107   |5     |
+------+------+
 */

    //7.将ResultDF存入到HBase标签结果表中
    resultDF.write
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.ROWKEY, "userId")
      .save()
    println("数据写入到HBase Success")
  }
}
