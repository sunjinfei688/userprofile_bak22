package cn.itcast.up.ml

import cn.itcast.up.base.BaseModel
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._

import scala.collection.immutable

/**
 * Author itcast
 * Date 2019/12/8 14:52
 * Desc 使用RFM+KMeans完成客户价值模型的开发
 * R:Rencency:最近一次消费时间距离今天的天数
 * F:Frequency:最近一段时间的消费频率(最近一段时间可能是一周/一月/半年...具体多久由运营/产品给定)
 * M:Monetary:最近一段时间的消费总金额(最近一段时间可能是一周/一月/半年...具体多久由运营/产品给定)
 * 因为要计算每个用户的RFM,所以去HBase中查数据时,应该要查询
 * memberId,orderSn,orderAmount,finishTime
 * 用户id,订单号,订单金额,订单完成时间
 */
object RFMModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }
  /**
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @return
   */
  override def getTagId(): Long = 37

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
|38 |1   |
|39 |2   |
|40 |3   |
|41 |4   |
|42 |5   |
|43 |6   |
|44 |7   |
+---+----+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)
+---------+-------------------+-----------+----------+
用户id      订单编号            订单金额      下单时间
+---------+-------------------+-----------+----------+
|memberId |orderSn            |orderAmount|finishTime|
+---------+-------------------+-----------+----------+
|13823431 |ts_792756751164275 |2479.45    |1564415022|
|4035167  |D14090106121770839 |2449.00    |1565687310|
|4035291  |D14090112394810659 |1099.42    |1564681801|
|4035041  |fx_787749561729045 |1999.00    |1565799378|
|13823285 |D14092120154435903 |2488.00    |1565062072|
|4034219  |D14092120155620305 |3449.00    |1563601306|
|138230939|top_810791455519102|1649.00    |1565509622|
|4035083  |D14092120161884409 |7.00       |1565731851|
|138230935|D14092120162313538 |1299.00    |1565382991|
|13823231 |D14092120162378713 |499.00     |1565677650|
+---------+-------------------+-----------+----------+
only showing top 10 rows

root
 |-- memberId: string (nullable = true)
 |-- orderSn: string (nullable = true)
 |-- orderAmount: string (nullable = true)
 |-- finishTime: string (nullable = true)
     */

    //0.导入隐式转换
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //0.定义常量字符串,避免后续拼写错误
    val recencyStr = "recency"
    val frequencyStr = "frequency"
    val monetaryStr = "monetary"
    val featureStr = "feature"
    val predictStr = "predict"

    //1.计算每个用户的RFM
    //R:Rencency:最近一次消费时间距离今天的天数,按照用户id分组后,使用SparkSQL的时间相关函数就可以求得
    //F:Frequency:最近一段时间的消费频率,按照用户id分组后,求count即可,count("orderSn")
    //M:Monetary:最近一段时间的消费总金额,按照用户id分组后,求sum即可,sum("orderAmount")
    val recencyColumn: Column = datediff(date_sub(current_timestamp(),100),from_unixtime(max('finishTime))).as(recencyStr)
    val frequencyColumn: Column = count('orderSn) as frequencyStr
    val monetaryColumn: Column = sum('orderAmount) as monetaryStr

    val tempDF: DataFrame = hbaseDF.groupBy('memberId)
      .agg(recencyColumn, frequencyColumn, monetaryColumn)
    //tempDF.show(10,false)
    //tempDF.printSchema()

    /*
 +---------+-------+---------+------------------+
|memberId |recency|frequency|monetary          |
+---------+-------+---------+------------------+
|13822725 |13    |116      |179298.34         |
|13823083 |13    |132      |233524.17         |
|138230919|13    |125      |240061.56999999998|
|13823681 |13    |108      |169746.1          |
|4033473  |13    |142      |251930.92         |
|13822841 |13    |113      |205931.91         |
|13823153 |13    |133      |250698.57         |
|13823431 |13    |122      |180858.22         |
|4033348  |13    |145      |240173.78999999998|
|4033483  |13    |110      |157811.09999999998|
+---------+-------+---------+------------------+
only showing top 10 rows

root
 |-- memberId: string (nullable = true)
 |-- recency: integer (nullable = true)
 |-- frequency: long (nullable = false)
 |-- monetary: double (nullable = true)
     */
    //2.根据用户RFM进行打分(自定义的归一化规则)
    //R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
    //F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
    //M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
    val recencyScore: Column = functions.when((col(recencyStr) >= 1) && (col(recencyStr) <= 3), 5)
      .when((col(recencyStr) >= 4) && (col(recencyStr) <= 6), 4)
      .when((col(recencyStr) >= 7) && (col(recencyStr) <= 9), 3)
      .when((col(recencyStr) >= 10) && (col(recencyStr) <= 15), 2)
      .when(col(recencyStr) >= 16, 1)
      .as(recencyStr)

    val frequencyScore: Column = functions.when(col(frequencyStr) >= 200, 5)
      .when((col(frequencyStr) >= 150) && (col(frequencyStr) <= 199), 4)
      .when((col(frequencyStr) >= 100) && (col(frequencyStr) <= 149), 3)
      .when((col(frequencyStr) >= 50) && (col(frequencyStr) <= 99), 2)
      .when((col(frequencyStr) >= 1) && (col(frequencyStr) <= 49), 1)
      .as(frequencyStr)

    val monetaryScore: Column = functions.when(col(monetaryStr) >= 200000, 5)
      .when(col(monetaryStr).between(100000, 199999), 4)
      .when(col(monetaryStr).between(50000, 99999), 3)
      .when(col(monetaryStr).between(10000, 49999), 2)
      .when(col(monetaryStr) <= 9999, 1)
      .as(monetaryStr)

    val RFMScoreDF: DataFrame = tempDF.select('memberId.as("userId"),recencyScore,frequencyScore,monetaryScore)
    //RFMScoreDF.show(10,false)
    //RFMScoreDF.printSchema()
    /*
 +---------+-------+---------+--------+
|userId   |recency|frequency|monetary|
+---------+-------+---------+--------+
|13822725 |2      |3        |4       |
|13823083 |2      |3        |5       |
|138230919|2      |3        |5       |
|13823681 |2      |3        |4       |
|4033473  |2      |3        |5       |
|13822841 |2      |3        |5       |
|13823153 |2      |3        |5       |
|13823431 |2      |3        |4       |
|4033348  |2      |3        |5       |
|4033483  |2      |3        |4       |
+---------+-------+---------+--------+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- recency: integer (nullable = true)
 |-- frequency: integer (nullable = true)
 |-- monetary: integer (nullable = true)
     */
    //====================================机器学习部分============================
    //3.特征工程-特征向量化
    //上面已经使用自定义的归一化规则进行打分搞定了,当然也可再继续使用MinMaxScaler进行归一化
    //所以在这我们就直接对数据进行-特征向量化就ok
    val vectorDF: DataFrame = new VectorAssembler()
      .setInputCols(Array(recencyStr, "frequency", "monetary"))
      .setOutputCol("vecotrFeatures")
      .transform(RFMScoreDF)
    //vectorDF.show(10,false)
    //vectorDF.printSchema()
    /*
 +---------+-------+---------+--------+--------------+
|userId   |recency|frequency|monetary|vecotrFeatures|
+---------+-------+---------+--------+--------------+
|13822725 |2      |3        |4       |[2.0,3.0,4.0] |
|13823083 |2      |3        |5       |[2.0,3.0,5.0] |
|138230919|2      |3        |5       |[2.0,3.0,5.0] |
|13823681 |2      |3        |4       |[2.0,3.0,4.0] |
|4033473  |2      |3        |5       |[2.0,3.0,5.0] |
|13822841 |2      |3        |5       |[2.0,3.0,5.0] |
|13823153 |2      |3        |5       |[2.0,3.0,5.0] |
|13823431 |2      |3        |4       |[2.0,3.0,4.0] |
|4033348  |2      |3        |5       |[2.0,3.0,5.0] |
|4033483  |2      |3        |4       |[2.0,3.0,4.0] |
+---------+-------+---------+--------+--------------+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- recency: integer (nullable = true)
 |-- frequency: integer (nullable = true)
 |-- monetary: integer (nullable = true)
 |-- vecotrFeatures: vector (nullable = true)
     */

    //4.创建模型并训练
    val model: KMeansModel = new KMeans()
      .setK(7) //K值应该是由算法工程师进行测试选取一些候选值,再结合运营/产品的要求来共同觉得K值到底是几!
      .setSeed(10) //随机种子
      .setMaxIter(10) //最大迭代次数
      .setFeaturesCol("vecotrFeatures") //设置特征列(向量化之后的)
      .setPredictionCol(predictStr) //设置预测列
      .fit(vectorDF)

    //5.进行聚类(预测)
    val result: DataFrame = model.transform(vectorDF)
    /*
+---------+-------+---------+--------+--------------+-------+
|userId   |recency|frequency|monetary|vecotrFeatures|predict|
+---------+-------+---------+--------+--------------+-------+
|13822725 |2      |3        |4       |[2.0,3.0,4.0] |1      |
|13823083 |2      |3        |5       |[2.0,3.0,5.0] |0      |
|138230919|2      |3        |5       |[2.0,3.0,5.0] |0      |
|13823681 |2      |3        |4       |[2.0,3.0,4.0] |1      |
|4033473  |2      |3        |5       |[2.0,3.0,5.0] |0      |
|13822841 |2      |3        |5       |[2.0,3.0,5.0] |0      |
|13823153 |2      |3        |5       |[2.0,3.0,5.0] |0      |
|13823431 |2      |3        |4       |[2.0,3.0,4.0] |1      |
|4033348  |2      |3        |5       |[2.0,3.0,5.0] |0      |
|4033483  |2      |3        |4       |[2.0,3.0,4.0] |1      |
+---------+-------+---------+--------+--------------+-------+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- recency: integer (nullable = true)
 |-- frequency: integer (nullable = true)
 |-- monetary: integer (nullable = true)
 |-- vecotrFeatures: vector (nullable = true)
 |-- predict: integer (nullable = true)
     */

    //6.查看结果
    //result.show(10,false)
    //result.printSchema()

    val ds: Dataset[Row] = result.groupBy(predictStr)//按照聚类编号进行分组
      //求每个聚类中的RFM的最大值和最小值
      .agg(max(col(recencyStr) + col(frequencyStr) + col(monetaryStr)), min(col(recencyStr) + col(frequencyStr) + col(monetaryStr)))
      .sort(col(predictStr).asc)//按照聚类编号的升序进行排序
    //ds.show(10,false)
    //result.printSchema()
    /*
 +-------+---------------------------------------+---------------------------------------+
|predict|max(((recency + frequency) + monetary))|min(((recency + frequency) + monetary))|
+-------+---------------------------------------+---------------------------------------+
|0      |10                                     |10                                     |
|1      |9                                      |9                                      |
|2      |9                                      |7                                      |
|3      |4                                      |3                                      |
|4      |6                                      |5                                      |
|5      |12                                     |11                                     |
|6      |4                                      |4                                      |
+-------+---------------------------------------+---------------------------------------+

root
 |-- userId: string (nullable = true)
 |-- recency: integer (nullable = true)
 |-- frequency: integer (nullable = true)
 |-- monetary: integer (nullable = true)
 |-- vecotrFeatures: vector (nullable = true)
 |-- predict: integer (nullable = true)
     */

    //问题:通过观察上面ds和fiveDF中的数据发现,聚类编号和客户价值等级编号没有顺序对应关系
    //那就意味着,不能直接将predict转为tagsId
    //该怎么办?
    //我们肯定要想办法将predict聚类编号和tagsId客户价值等级编号进行一一对应!
    //所以接下来就应该去解决这个问题!

    //7.将predict聚类编号和tagsId客户价值等级编号进行一一对应
    //7.1先求出各聚类中心的RFM的和作为value,聚类编号作为key--> [predict聚类编号,聚类中心RFM的和]
    val indexAndSum: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices.map(predictIndex => {
      val RFMSum: Double = model.clusterCenters(predictIndex).toArray.sum
      (predictIndex, RFMSum)
    })
    //val indexAndSum2: immutable.IndexedSeq[(Int, Double)] = for (i <- model.clusterCenters.indices) yield (i,model.clusterCenters(i).toArray.sum)
    //indexAndSum.foreach(println)
    /*
(0,10.0)
(1,9.0)
(2,8.0)
(3,3.142857142857143)
(4,5.2)
(5,11.038461538461538)
(6,4.0)
     */

    //7.2对indexAndSum按照聚类中心RFM的和进行排序
    val sortedIndexAndSum: immutable.IndexedSeq[(Int, Double)] = indexAndSum.sortBy(_._2).reverse
    //sortedIndexAndSum.foreach(println)
    /*
sortedIndexAndSum
(5,11.038461538461538)
(0,10.0)
(1,9.0)
(2,8.0)
(4,5.2)
(6,4.0)
(3,3.142857142857143)
     */

    //7.3将sortedIndexAndSum和fiveDF进行拉链!
    /*
  fiveDF
+---+----+
|id |rule|
+---+----+
|38 |1   |
|39 |2   |
|40 |3   |
|41 |4   |
|42 |5   |
|43 |6   |
|44 |7   |
+---+----+
     */
    //目标是得到如下结果
    /*
----------------
predict |tagsId|
---------------
 5     | 38    |
 0     | 39    |
 1     | 40    |
 2     | 41    |
 4     | 42    |
 6     | 43    |
 3     | 44    |
 ---  ---+------
     */
    val ruleArr: Array[(Long, String)] = fiveDF.as[(Long,String)].collect().sortBy(_._1)
    //[((predict聚类编号, RFM和), (tagsId, rule))]
    val tempSeq: immutable.IndexedSeq[((Int, Double), (Long, String))] = sortedIndexAndSum.zip(ruleArr)
    //Map[predict, tagsId]
    val predictAndTagsIdMap: Map[Int, Long] = tempSeq.map(t => {
      (t._1._1, t._2._1)
    }).toMap
    predictAndTagsIdMap.foreach(println)
/*
(0,39)
(5,38)
(1,40)
(6,43)
(2,41)
(3,44)
(4,42)
 */

    //8.将result中的predict转换成tagsId
    val predict2tagsId = udf((predict:Int)=>{
      predictAndTagsIdMap(predict)
    })

    val newDF: DataFrame = result.select('userId,predict2tagsId('predict).as("tagsId"))
    newDF.show(10,false)
    /*
 +---------+------+
|userId   |tagsId|
+---------+------+
|13822725 |40    |
|13823083 |39    |
|138230919|39    |
|13823681 |40    |
|4033473  |39    |
|13822841 |39    |
|13823153 |39    |
|13823431 |40    |
|4033348  |39    |
|4033483  |40    |
+---------+------+
     */

    null
  }
}
