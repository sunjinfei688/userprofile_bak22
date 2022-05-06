package cn.itcast.up.ml

import cn.itcast.up.base.BaseModel
import cn.itcast.up.common.HDFSUtils
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.collection.{immutable, mutable}

/**
 * Author itcast
 * Date 2019/12/10 14:58
 * Desc 使用RFE客户活跃度模型+KMeans对用户建模
 * R:Recency最近一次访问时间距离今天的天数
 * F:Frequency最近一段时间的访问频率
 * E:Engagements最近一段时间的页面互动度:浏览数/点击数/点赞数/收藏数/转发数
 */
object RFEModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }
  /**
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @return
   */
  override def getTagId(): Long = 45

  /**
   * 根据5级标签和HBase数据进行标签计算(每个模型/标签计算方式不同)
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
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
+---+----+
|id |rule|
+---+----+
|46 |1   |
|47 |2   |
|48 |3   |
|49 |4   |
+---+----+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)
用户id            访问的url                                                            访问时间
+--------------+-------------------------------------------------------------------+-------------------+
|global_user_id|loc_url                                                            |log_time           |
+--------------+-------------------------------------------------------------------+-------------------+
|424           |http://m.eshop.com/mobile/coupon/getCoupons.html?couponsId=3377    |2019-08-13 03:03:55|
|619           |http://m.eshop.com/?source=mobile                                  |2019-07-29 15:07:41|
|898           |http://m.eshop.com/mobile/item/11941.html                          |2019-08-14 09:23:44|
|642           |http://www.eshop.com/l/2729-2931.html                              |2019-08-11 03:20:17|
|130           |http://www.eshop.com/                                              |2019-08-12 11:59:28|
|515           |http://www.eshop.com/l/2723-0-0-1-0-0-0-0-0-0-0-0.html             |2019-07-23 14:39:25|
|274           |http://www.eshop.com/                                              |2019-07-24 15:37:12|
|772           |http://ck.eshop.com/login.html                                     |2019-07-24 07:56:49|
|189           |http://m.eshop.com/mobile/item/9673.html                           |2019-07-26 19:17:00|
|529           |http://m.eshop.com/mobile/search/_bplvbiwq_XQS75_btX_ZY1328-se.html|2019-07-25 23:18:37|
+--------------+-------------------------------------------------------------------+-------------------+
only showing top 10 rows
 */
    //0.导入隐式转换
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //0.定义常量字符串,避免后续拼写错误
    val recencyStr = "recency"
    val frequencyStr = "frequency"
    val engagementsStr = "engagements"
    val featureStr = "feature"
    val scaleFeatureStr = "scaleFeature"
    val predictStr = "predict"

    //1.按照用户id进行分组,求出每个用户的RFE
    //R:Recency最近一次访问时间距离今天的天数
    //F:Frequency最近一段时间的访问频率
    //E:Engagements最近一段时间的页面互动度:访问的独立页面数(去重计数)/浏览数/点击数/点赞数/收藏数/转发数
    val recencyColumn: Column = datediff(date_sub(current_date(),70),max('log_time)) as recencyStr
    val frequencyColumn: Column = count('loc_url) as frequencyStr
    val engagementsColumn: Column = countDistinct('loc_url) as engagementsStr

    val tempDF: DataFrame = hbaseDF.groupBy('global_user_id)
      .agg(recencyColumn, frequencyColumn, engagementsColumn)
    //tempDF.show(10,false)
    //tempDF.printSchema()
    /*
 +--------------+-------+---------+-----------+
|global_user_id|recency|frequency|engagements|
+--------------+-------+---------+-----------+
|296           |45    |380      |227        |
|467           |45    |405      |267        |
|675           |45    |370      |240        |
|691           |45    |387      |244        |
|829           |45    |404      |269        |
|125           |45    |375      |246        |
|451           |45    |347      |224        |
|800           |45    |395      |242        |
|853           |45    |388      |252        |
|944           |45    |394      |252        |
+--------------+-------+---------+-----------+
only showing top 10 rows

root
 |-- global_user_id: string (nullable = true)
 |-- recency: integer (nullable = true)
 |-- frequency: long (nullable = false)
 |-- engagements: long (nullable = false)
     */

    //2.打分
    // R:0-15天=5分，16-30天=4分，31-45天=3分，46-60天=2分，大于61天=1分
    // F:≥400=5分，300-399=4分，200-299=3分，100-199=2分，≤99=1分
    // E:≥250=5分，230-249=4分，210-229=3分，200-209=2分，1=1分
    val recencyScore: Column = when(col(recencyStr).between(0, 15), 5)
      .when(col(recencyStr).between(16, 30), 4)
      .when(col(recencyStr).between(31, 45), 3)
      .when(col(recencyStr).between(46, 60), 2)
      .when(col(recencyStr).gt(60), 1)
      .as(recencyStr)

    val frequencyScore: Column = when(col(frequencyStr).geq(400), 5)
      .when(col(frequencyStr).between(300, 399), 4)
      .when(col(frequencyStr).between(200, 299), 3)
      .when(col(frequencyStr).between(100, 199), 2)
      .when(col(frequencyStr).leq(99), 1)
      .as(frequencyStr)

    val engagementsScore: Column = when(col(engagementsStr).geq(250), 5)
      .when(col(engagementsStr).between(200, 249), 4)
      .when(col(engagementsStr).between(150, 199), 3)
      .when(col(engagementsStr).between(50, 149), 2)
      .when(col(engagementsStr).leq(49), 1)
      .as(engagementsStr)

    val RFEScoreResult =tempDF.select('global_user_id as "userId", recencyScore, frequencyScore, engagementsScore)
      .where('userId.isNotNull and col(recencyStr).isNotNull and col(frequencyStr).isNotNull and col(engagementsStr).isNotNull)
    //RFEScoreResult.show(10,false)
    //RFEScoreResult.printSchema()
/*
+------+-------+---------+-----------+
|userId|recency|frequency|engagements|
+------+-------+---------+-----------+
|296   |3      |4        |4          |
|467   |3      |5        |5          |
|675   |3      |4        |4          |
|691   |3      |4        |4          |
|829   |3      |5        |5          |
|125   |3      |4        |4          |
|451   |3      |4        |4          |
|800   |3      |4        |4          |
|853   |3      |4        |5          |
|944   |3      |4        |5          |
+------+-------+---------+-----------+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- recency: integer (nullable = true)
 |-- frequency: integer (nullable = true)
 |-- engagements: integer (nullable = true)

 */
    //3.特征工程--特征向量化
    val vectorDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("recency", "frequency", "engagements"))
      .setOutputCol("vectorFeatures")
      .transform(RFEScoreResult)
    vectorDF.show(10,false)
    vectorDF.printSchema()
    /*
 +------+-------+---------+-----------+--------------+
|userId|recency|frequency|engagements|vectorFeatures|
+------+-------+---------+-----------+--------------+
|296   |3      |4        |4          |[3.0,4.0,4.0] |
|467   |3      |5        |5          |[3.0,5.0,5.0] |
|675   |3      |4        |4          |[3.0,4.0,4.0] |
|691   |3      |4        |4          |[3.0,4.0,4.0] |
|829   |3      |5        |5          |[3.0,5.0,5.0] |
|125   |3      |4        |4          |[3.0,4.0,4.0] |
|451   |3      |4        |4          |[3.0,4.0,4.0] |
|800   |3      |4        |4          |[3.0,4.0,4.0] |
|853   |3      |4        |5          |[3.0,4.0,5.0] |
|944   |3      |4        |5          |[3.0,4.0,5.0] |
+------+-------+---------+-----------+--------------+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- recency: integer (nullable = true)
 |-- frequency: integer (nullable = true)
 |-- engagements: integer (nullable = true)
 |-- vectorFeatures: vector (nullable = true)
     */

    //4.创建模型并训练--K值的选取
    //准备备选的K值
    /*val list:List[Int] = List(2,3,4,5,6,7)
    //准备Map存K值对应的SSE
    val map = mutable.Map[Int,Double]()
    for(k <- list){
      val model: KMeansModel = new KMeans()
        .setK(k) //设置K
        .setSeed(10) //随机种子
        .setMaxIter(10) //最大迭代次数
        .setFeaturesCol("vectorFeatures")
        .setPredictionCol("predict")
        .fit(vectorDF)
      //计算K值对应的SSE //sum of squared distances of points to their nearest center
      val sse: Double = model.computeCost(vectorDF)
      map.put(k,sse)
    }
    //打印所有备选的K值对应的SSE并画图根据肘部法则选取K值
    map.toArray.sortBy(_._1).foreach(println)*/
    /*
(2,185.03108672935983)
(3,23.668965517242555)
(4,0.0)
(5,0.0)
(6,0.0)
(7,0.0)
根据上面的K值和对应的SSE的值,得到较为合适的K为4
     */

    //根据上面的K值和对应的SSE的值,得到较为合适的K为4进行模型的训练
    //模型的保存和加载
    //第一次需要训练
    //以后可以直接加载用该模型(直到该模型过期,如一个月重新训练一次)
    //所以这里我们需要将训练好的模型进行持久化的保存,下次使用的时候直接加载,直到该模型过期,将持久化的该模型删除
    val path:String = "/model/RFE"
    var model:KMeansModel = null
    if(HDFSUtils.getInstance().exists(path)){
      //目录存在,不是第一次,应该直接加载该模型
      println("目录存在,不是第一次,应该直接加载该模型")
      model = KMeansModel.load(path)
    }else{
      //目录不存在,是第一次,需要创建模型并训练,最后将模型保存到path
      println("目录不存在,是第一次,需要创建模型并训练,最后将模型保存到path")
      model = new KMeans()
        .setK(4) //设置K
        .setSeed(10) //随机种子
        .setMaxIter(10) //最大迭代次数
        .setFeaturesCol("vectorFeatures")
        .setPredictionCol("predict")
        .fit(vectorDF)
      model.save(path)
    }

    //5.使用模型进行预测/聚类
    val result: DataFrame = model.transform(vectorDF)

    //==================================================
    //6.查看结果
    //7.取出[聚类中心编号,聚类中心RFE的SUM]并按照SUM排序
    //8.与fiveDF进行拉链Zip
    //==================================================
    //6.查看结果
    //result.show(10,false)
    //result.printSchema()

    val ds: Dataset[Row] = result.groupBy(predictStr)//按照聚类编号进行分组
      //求每个聚类中的RFM的最大值和最小值
      .agg(max(col(recencyStr) + col(frequencyStr) + col(engagementsStr)), min(col(recencyStr) + col(frequencyStr) + col(engagementsStr)))
      .sort(col(predictStr).asc)//按照聚类编号的升序进行排序
    //ds.show(10,false)
    //result.printSchema()
    /*

     */

    //问题:通过观察上面ds和fiveDF中的数据发现,聚类编号和客户价值等级编号没有顺序对应关系
    //那就意味着,不能直接将predict转为tagsId
    //该怎么办?
    //我们肯定要想办法将predict聚类编号和tagsId客户价值等级编号进行一一对应!
    //所以接下来就应该去解决这个问题!

    val predictAndTagsIdMap: _root_.scala.Predef.Map[Int, Long] = zip(fiveDF, model)
    predictAndTagsIdMap.foreach(println)
    /*

     */

    //8.将result中的predict转换成tagsId
    val predict2tagsId = udf((predict:Int)=>{
      predictAndTagsIdMap(predict)
    })

    val newDF: DataFrame = result.select('userId,predict2tagsId('predict).as("tagsId"))
    newDF.show(10,false)


    null
  }


}
