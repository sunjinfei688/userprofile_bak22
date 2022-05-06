package cn.itcast.up.statistics

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession, functions}

/**
 * Author itcast
 * Date 2019/12/7 9:23
 * Desc 完成统计型标签-年龄段标签/模型的开发
 */
object AgeModel {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("AgeModel")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import org.apache.spark.sql.functions._
    import spark.implicits._
    //1.获取MySQL数据
    val url:String = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val table:String = "tbl_basic_tag"
    val properties:Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url,table,properties)

    //2.获取4级标签,并解析
    //rule
    //inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,birthday
    val fourDF: Dataset[Row] = mysqlDF.select('rule).where('id === 14)
    val fourMap: Map[String, String] = fourDF.map(row => {//每一行
      val ruleStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = ruleStr.split("##") //[#][#]
      val tupleArr: Array[(String, String)] = kvs.map(kv => {
        val kvArr: Array[String] = kv.split("=")
        (kvArr(0), kvArr(1))
      })
      tupleArr
    }).collectAsList().get(0).toMap
    val hbaseMeta = HBaseMeta(fourMap)

    //3.获取5级标签-fiveDF
    val fiveDF: Dataset[Row] = mysqlDF.select('id,'rule).where('pid===14)
    //fiveDF.show(10,false)
    //fiveDF.printSchema()
    /*
 +---+-----------------+
|id |rule             |
+---+-----------------+
|15 |19500101-19591231|
|16 |19600101-19691231|
|17 |19700101-19791231|
|18 |19800101-19891231|
|19 |19900101-19991231|
|20 |20000101-20091231|
|21 |20100101-20191231|
|22 |20200101-20291231|
+---+-----------------+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)
     */

    //4.根据4级标签获取HBase数据-hbaseDF
    val hbaseDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .load()
    //hbaseDF.show(10,false)
    //hbaseDF.printSchema()
    /*
 +---+----------+
|id |birthday  |
+---+----------+
|1  |1992-05-31|
|10 |1980-10-13|
|100|1993-10-28|
|101|1996-08-18|
|102|1996-07-28|
|103|1987-05-13|
|104|1976-05-08|
|105|1983-10-11|
|106|1984-03-19|
|107|1998-12-29|
+---+----------+
only showing top 10 rows

root
 |-- id: string (nullable = true)
 |-- birthday: string (nullable = true)
     */

    //5.根据fiveDF和hbaseDF计算结果-newDF
    //5.1统一格式,将birthday:1992-05-31转为19920531
    val birthdayColumn: Column = functions.regexp_replace('birthday,"-","")
    val tempDF: DataFrame = hbaseDF.select('id,birthdayColumn.as("birthday"))
    //hbaseDF.select('id,functions.regexp_replace('birthday,"-",""))
    tempDF.show(10,false)
/*
+---+--------+
|id |birthday|
+---+--------+
|1  |19920531|
|10 |19801013|
|100|19931028|
|101|19960818|
|102|19960728|
|103|19870513|
|104|19760508|
|105|19831011|
|106|19840319|
|107|19981229|
+---+--------+
 */
    //5.2切割列,一列边两列,将fiveDF中的rule列:19500101-19591231,变为start:19500101和end:19591231
    val rangDF: DataFrame = fiveDF.as[(Long, String)].map(t => {
      val arr: Array[String] = t._2.split("-")
      (t._1, arr(0), arr(1))
    }).toDF("tagsId", "start", "end")
    rangDF.show(10,false)
    /*fiveDF.map(row=>{
      val id: Long = row.getAs[Long]("id")
      val rule: String = row.getAs[String]("rule")
      val arr: Array[String] = rule.split("-")
      (id,arr(0),arr(1))
    }).toDF("tagsId", "start", "end")*/
/*
+------+--------+--------+
|tagsId|start   |end     |
+------+--------+--------+
|15    |19500101|19591231|
|16    |19600101|19691231|
|17    |19700101|19791231|
|18    |19800101|19891231|
|19    |19900101|19991231|
|20    |20000101|20091231|
|21    |20100101|20191231|
|22    |20200101|20291231|
+------+--------+--------+
 */
    //5.3比较大小tempDF.birthday.between(start,end)
    val newDF: DataFrame = tempDF.join(rangDF)
      .where(tempDF.col("birthday").between(rangDF.col("start"), rangDF.col("end")))
      .select(tempDF.col("id").as("userId"), rangDF.col("tagsId"))
    newDF.show(10,false)
    /*
+------+------+
|userId|tagsId|
+------+------+
|1     |19    |
|10    |18    |
|100   |19    |
|101   |19    |
|102   |19    |
|103   |18    |
|104   |17    |
|105   |18    |
|106   |18    |
|107   |19    |
+------+------+
     */


    //6.将oldDF和newDF进行合并去重得到最终结果-result
    val oldDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .load()

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
    val sql:String =
      """
        |select n.userId,meger(n.tagsId,o.tagsId) as tagsId
        |from t_new n
        |left join t_old o
        |on n.userId = o.userId
        |""".stripMargin
    val resultDF: DataFrame = spark.sql(sql)

    //7.将result存入HBase画像表中
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
