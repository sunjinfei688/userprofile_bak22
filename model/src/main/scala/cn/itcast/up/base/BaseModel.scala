package cn.itcast.up.base

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import cn.itcast.up.ml.RFEModel2.spark
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable

/**
 * Author itcast
 * Date 2019/12/7 10:53
 * Desc trait相当于Java中的接口
 * Java中的接口是用来定义规范的,并可以由实现类进行功能的扩展
 * 在Scala中也是一样
 * 而我们进行代码重构的目的就是将模型/标签开发的重复的步骤进行抽取和规范
 * 并让子类/实现类进行具体的计算流程的实现/扩展
 * 所以可以选用trait
 * 在该trait中,将模型/标签开发的共同操作进行封装/规范
 * 每个标签不同的操作,让子类/实现类进行扩展/实现
 * 哪些是共同的操作?
 * 0,1,2,3,4,6,7
 * 哪些是不同的操作?
 * 5
 */
trait BaseModel {
  //0.封装参数
  val config: Config = ConfigFactory.load()
  val url: String = config.getString("jdbc.url")
  val tableName: String = config.getString("jdbc.table")
  val sourceClass: String = config.getString("hbase.source.class")
  val zkHosts: String = config.getString("hbase.source.zkHosts")
  val zkPort: String = config.getString("hbase.source.zkPort")
  val hbaseTable: String = config.getString("hbase.source.hbaseTable")
  val family: String = config.getString("hbase.source.family")
  val selectFields: String = config.getString("hbase.source.selectFields")
  val rowKey: String = config.getString("hbase.source.rowKey")

  val hbaseMeta = HBaseMeta(
    "",
    zkHosts,
    zkPort,
    hbaseTable,
    family,
    selectFields,
    rowKey
  )

  //0.创建sparksession
  val spark = SparkSession.builder()
    .appName("model")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._
  import scala.collection.JavaConversions._
  import org.apache.spark.sql.functions._




  /**
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @return
   */
  def getTagId(): Long

  /**
   * 1.查询tbl_basic_tag表中的数据
   * @return
   */
  def getMySQLData(): DataFrame = {
    val url:String = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val table:String = "tbl_basic_tag"
    val properties:Properties = new Properties()
    spark.read.jdbc(url,table,properties)
  }



  /**
   * 2.根据4级标签id查询MySQL中的数据
   * @return
   */
  def getFourRule(mysqlDF: DataFrame): HBaseMeta = {
    val fourDF: Dataset[Row] = mysqlDF.select('rule).where('id === getTagId())
    val fourMap: Map[String, String] = fourDF.map(row => {//每一行
      val ruleStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = ruleStr.split("##") //[#][#]
      val tupleArr: Array[(String, String)] = kvs.map(kv => {
        val kvArr: Array[String] = kv.split("=")
        (kvArr(0), kvArr(1))
      })
      tupleArr
    }).collectAsList().get(0).toMap
    HBaseMeta(fourMap)
  }

  /**
   * 3.根据4级标签id作为5级标签的父id,进行查询
   * @param mysqlDF
   * @return
   */
  def getFiveDF(mysqlDF: DataFrame): DataFrame = {
    mysqlDF.select('id,'rule).where('pid===getTagId())
  }

  /**
   * 4.根据hbaseMeta查询HBase数据
   * @param hbaseMeta
   * @return
   */
  def getHBaseDF(hbaseMeta: HBaseMeta): DataFrame = {
    spark.read
      .format(sourceClass)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .load()
  }

  /**
   * 5.根据5级标签和HBase数据进行标签计算(每个模型/标签计算方式不同)
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @param fiveDF
   * @param hbaseDF
   * @return
   */
  def compute(fiveDF:DataFrame,hbaseDF:DataFrame):DataFrame


  /**
   * 6.将新旧结果集进行合并
   * @param newDF
   * @return
   */
  def meger(newDF: DataFrame): DataFrame = {
    val oldDF: DataFrame = spark.read
      .format(sourceClass)
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .load()
    //合并去重
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
    spark.sql(sql)
  }

  /**
   * 7.保存结果
   * @param result
   */
  def save(result: DataFrame): Unit = {
    result.write
      .format(sourceClass)
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.ROWKEY, "userId")
      .save()
    println("数据写入到HBase Success")
  }

  /**
   * 模版方法设计模式
   * execute方法中规范了标签/模型计算的整体流程,并对重复的流程进行了封装
   * 每一个标签/模型不一样的操作,应该由子类进行具体的实现/扩展
   * 那么子类在进行标签/模型的计算的时候只需要实现该trait,并在main方法中调用execute方法
   * 并实现抽象方法compute和getTagId即可
   */
  def execute():Unit={
    //1.读取MySQL数据
    val mysqlDF:DataFrame = getMySQLData()
    //2.读取4级标签并解析
    val hbaseMeta:HBaseMeta = getFourRule(mysqlDF)
    //3.读取5级标签
    val fiveDF:DataFrame = getFiveDF(mysqlDF)
    //4.根据4级标签读取HBase数据
    val hbaseDF:DataFrame = getHBaseDF(hbaseMeta)
    //5.根据5级标签和HBase数据进行标签计算(每个模型/标签计算方式不同)
    val newDF:DataFrame = compute(fiveDF,hbaseDF)
    //6.将新的结果和旧的结果进行合并
    val result:DataFrame = meger(newDF)
    //7.将最终的结果写入HBase
    save(result)
  }

  def zipAndTag(fiveDF: DataFrame, model: KMeansModel, result: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //7.将predict聚类编号和tagsId客户价值等级编号进行一一对应
    //7.1先求出各聚类中心的RFM的和作为value,聚类编号作为key--> [predict聚类编号,聚类中心RFM的和]
    val indexAndSum: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices.map(predictIndex => {
      val RFMSum: Double = model.clusterCenters(predictIndex).toArray.sum
      (predictIndex, RFMSum)
    })
    //val indexAndSum2: immutable.IndexedSeq[(Int, Double)] = for (i <- model.clusterCenters.indices) yield (i,model.clusterCenters(i).toArray.sum)
    //indexAndSum.foreach(println)

    //7.2对indexAndSum按照聚类中心RFM的和进行排序
    val sortedIndexAndSum: immutable.IndexedSeq[(Int, Double)] = indexAndSum.sortBy(_._2).reverse
    //sortedIndexAndSum.foreach(println)

    //7.3将sortedIndexAndSum和fiveDF进行拉链!
    val ruleArr: Array[(Long, String)] = fiveDF.as[(Long, String)].collect().sortBy(_._1)
    //[((predict聚类编号, RFM和), (tagsId, rule))]
    val tempSeq: immutable.IndexedSeq[((Int, Double), (Long, String))] = sortedIndexAndSum.zip(ruleArr)
    //Map[predict, tagsId]
    val predictAndTagsIdMap: Map[Int, Long] = tempSeq.map(t => {
      (t._1._1, t._2._1)
    }).toMap
    predictAndTagsIdMap.foreach(println)

    //8.将result中的predict转换成tagsId

    val predict2tagsId = udf((predict: Int) => {
      predictAndTagsIdMap(predict)
    })

    val newDF: DataFrame = result.select('userId, predict2tagsId('predict).as("tagsId"))
    newDF.show(10, false)
    newDF
  }

  def zip(fiveDF: DataFrame, model: KMeansModel): Map[Int, Long] = {
    //7.将predict聚类编号和tagsId客户价值等级编号进行一一对应
    //7.1先求出各聚类中心的RFM的和作为value,聚类编号作为key--> [predict聚类编号,聚类中心RFM的和]
    val indexAndSum: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices.map(predictIndex => {
      val RFMSum: Double = model.clusterCenters(predictIndex).toArray.sum
      (predictIndex, RFMSum)
    })
    //val indexAndSum2: immutable.IndexedSeq[(Int, Double)] = for (i <- model.clusterCenters.indices) yield (i,model.clusterCenters(i).toArray.sum)
    //indexAndSum.foreach(println)

    //7.2对indexAndSum按照聚类中心RFM的和进行排序
    val sortedIndexAndSum: immutable.IndexedSeq[(Int, Double)] = indexAndSum.sortBy(_._2).reverse
    //sortedIndexAndSum.foreach(println)

    //7.3将sortedIndexAndSum和fiveDF进行拉链!
    //目标是得到如下结果
    val ruleArr: Array[(Long, String)] = fiveDF.as[(Long, String)].collect().sortBy(_._1)
    //[((predict聚类编号, RFM和), (tagsId, rule))]
    val tempSeq: immutable.IndexedSeq[((Int, Double), (Long, String))] = sortedIndexAndSum.zip(ruleArr)
    //Map[predict, tagsId]
    val predictAndTagsIdMap: Map[Int, Long] = tempSeq.map(t => {
      (t._1._1, t._2._1)
    }).toMap
    predictAndTagsIdMap
  }
}
