package cn.itcast.up.matchtag

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author itcast
 * Date 2019/12/5 14:44
 * Desc 完成职业标签开发
 */
object JobModel2 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("JobModel")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    //1.读取MySQL数据-mysqlDF
    val url:String = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val table:String = "tbl_basic_tag"
    val properties:Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url,table,properties)

    //2.从mysqlDF获取4级标签规则
    //rule
    //inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,job
    val fourRuleDS: Dataset[Row] = mysqlDF.select("rule").where("id=7")

    //3.从mysqlDF获取5级标签规则--fiveDF
    val fiveDF: Dataset[Row] = mysqlDF.select("id","rule").where("pid=7")
    //fiveDF.show(10,false)
    /*
+---+----+
|id |rule|
+---+----+
|8  |1   |
|9  |2   |
|10 |3   |
|11 |4   |
|12 |5   |
|13 |6   |
+---+----+
     */

    //4.解析4级标签规则
    val tempDS: Dataset[Array[(String, String)]] = fourRuleDS.map(row => {
      val ruleStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = ruleStr.split("##")
      kvs.map(kv => {
        val kvArr: Array[String] = kv.split("=")
        (kvArr(0), kvArr(1))
      })
    })
    val tempArr: Array[Array[(String, String)]] = tempDS.collect()
    val tempArr2: Array[(String, String)] = tempArr(0)
    val fourRuleMap: Map[String, String] = tempArr2.toMap

    //为了方便后续使用,将fourRleMap转为HBaseMate样例类
    val hbaseMeta: HBaseMeta = HBaseMeta(fourRuleMap)

    //5.根据4级标签规则去HBase中查询数据--hbaseDF
    //spark.read.jdbc()
    //spark.read.format("jdbc")
    val hbaseDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      //HBaseMeta.SELECTFIELDS 字符串常量
      //hbaseMeta.selectFields 样例类对象中的字段值
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .load()
    //hbaseDF.show(10,false)
/*
+---+---+
|id |job|
+---+---+
|1  |3  |
|10 |5  |
|100|3  |
|101|1  |
|102|1  |
|103|3  |
|104|6  |
|105|2  |
|106|4  |
|107|1  |
+---+---+
 */

    //6.将hbaseDF和fiveDF进行关联,得到结果
    // Map[job, tagId]
    val fiveRuleMap: Map[String, Long] = fiveDF.as[(Long, String)].map(t => {
      (t._2, t._1)
    }).collect().toMap

    spark.udf.register("job2tag",(job:String)=>{
      fiveRuleMap(job)
    })

    hbaseDF.createOrReplaceTempView("t_hbase")
    val newDF: DataFrame = spark.sql("select id as userId, job2tag(job) as tagsId from t_hbase")
    //newDF.show(10,false)
    /*
+------+------+
|userId|tagsId|
+------+------+
|1     |10 ,   |
|10    |12 ,   |
|100   |10 ,   |
|101   |8  ,   |
|102   |8  ,   |
|103   |10 ,   |
|104   |13 ,   |
|105   |9  ,   |
|106   |11 ,   |
|107   |8  ,   |
+------+------+
     */


    //注意:不能直接将newDF写入到HBase,因为这样会覆盖之前的数据
    //那么该如何做?
    //那么我们这里用一种简单的办法,将之前的结果查出来oldDF和现在的结果newDF进行合并,得到最终的结果resultDF,再写入到HBase
    //7.查询oldDF
    val oldDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .load()
    //oldDF.show(10,false)
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

    //8.将newDF和oldDF进行join--SQL风格
    newDF.createOrReplaceTempView("t_new")
    oldDF.createOrReplaceTempView("t_old")

    //自定义UDF--SQL风格
    spark.udf.register("meger",(newTagsId:String,oldTagsId:String)=>{
      if (StringUtils.isBlank(newTagsId)){
        oldTagsId
      }else if (StringUtils.isBlank(oldTagsId)){
        newTagsId
      }else{
        (newTagsId.split(",") ++ oldTagsId.split(",")).toSet.mkString(",")
      }
    })

    /*
    情况一:new和old表中的userId一样
    new表:
    userId
    1
    2
    3
    old表:
    userId
    1
    2
    3
    result表:
    userId
    1
    2
    3
    情况二:new和old表中的userId不一样
    new表:
    userId
    1
    2
    3
    4
    old表:
    userId
    1
    2
    3
    result表:
    userId
    1
    2
    3
    4
    情况三:new和old表中的userId不一样
    new表:
    userId
    1
    2
    3
    old表:
    userId
    1
    2
    3
    4
    result表:
    userId
    1
    2
    3
     */
    val sql:String =
      """
        |select n.userId,meger(n.tagsId,o.tagsId) as tagsId
        |from t_new n
        |left join t_old o
        |on n.userId = o.userId
        |""".stripMargin
    val resultDF: DataFrame = spark.sql(sql)


    //9.将结果写入到HBase
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
