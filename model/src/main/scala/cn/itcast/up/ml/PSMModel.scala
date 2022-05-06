package cn.itcast.up.ml

import cn.itcast.up.base.BaseModel
import cn.itcast.up.common.HDFSUtils
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Author itcast
 * Date 2019/12/11 10:05
 * Desc 使用KMeans算法完成用户价格敏感度模型PSM的建立
 * psm = 优惠订单占比 + 平均优惠金额占比 + 优惠总金额占比   (运营/产品提供)
 * psm = 优惠订单数/总订单数 + 平均优惠金额/平均每单应收金额 + 优惠总金额/应收总金额
 * psm = 优惠订单数/总订单数 + ((优惠总金额/优惠订单数)/(应收总金额/总订单数)) + 优惠总金额/应收总金额
 * 需要算: 优惠订单数、总订单数、优惠总金额、应收总金额  //优惠订单数可能为0
 */
object PSMModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  /**
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @return
   */
  override def getTagId(): Long = 50

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
|51 |1   |
|52 |2   |
|53 |3   |
|54 |4   |
|55 |5   |
+---+----+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)
用户id      订单编号            订单金额(实收金额)   优惠金额
+---------+-------------------+-----------+---------------+
|memberId |orderSn            |orderAmount|couponCodeValue|
+---------+-------------------+-----------+---------------+
|13823431 |ts_792756751164275 |2479.45    |0.00           |
|4035167  |D14090106121770839 |2449.00    |0.00           |
|4035291  |D14090112394810659 |1099.42    |0.00           |
|4035041  |fx_787749561729045 |1999.00    |0.00           |
|13823285 |D14092120154435903 |2488.00    |0.00           |
|4034219  |D14092120155620305 |3449.00    |0.00           |
|138230939|top_810791455519102|1649.00    |0.00           |
|4035083  |D14092120161884409 |7.00       |0.00           |
|138230935|D14092120162313538 |1299.00    |0.00           |
|13823231 |D14092120162378713 |499.00     |0.00           |
+---------+-------------------+-----------+---------------+
only showing top 10 rows

root
 |-- memberId: string (nullable = true)
 |-- orderSn: string (nullable = true)
 |-- orderAmount: string (nullable = true)
 |-- couponCodeValue: string (nullable = true)
     */
    //0.定义字符串常量,避免拼写错误
    val psmScoreStr: String = "psmScore"
    val featureStr: String = "feature"
    val predictStr: String = "predict"

    //0.导入隐式转换
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //1.计算每个用户的PSM
    //psm = 优惠订单占比 + 平均优惠金额占比 + 优惠总金额占比   (运营/产品提供)
    //psm = 优惠订单数/总订单数 + 平均优惠金额/平均每单应收金额 + 优惠总金额/应收总金额
    //psm = 优惠订单数/总订单数 + ((优惠总金额/优惠订单数)/(应收总金额/总订单数)) + 优惠总金额/应收总金额
    //需要算: 优惠订单数、总订单数、优惠总金额、应收总金额  //优惠订单数可能为0

    //ra:receivableAmount 应收金额   110
    val raColumn = 'couponCodeValue + 'orderAmount as "ra"
    //da:discountAmount 优惠金额   10
    val daColumn = 'couponCodeValue  as "da"
    //pa:practicalAmount 实收金额   100
    val paColumn = 'orderAmount as "pa"
    //state 优惠订单为1, 非优惠订单为0
    //优惠金额不为0,则为优惠订单,记为1; 优惠金额为0,则为非优惠订单,记为0
    val stateColumn: Column = when('couponCodeValue =!= 0, 1)
      .when('couponCodeValue === 0, 0)
      .as("state")

    val tempDF: DataFrame = hbaseDF.select('memberId as "userId",raColumn,daColumn,paColumn,stateColumn)
    //tempDF.show(10,false)
    //tempDF.printSchema()
    /*
用户id     应收金额 优惠金额 实收金额 订单状态(优惠订单为1, 非优惠订单为0)
+---------+-------+----+-------+-----+
|userId   |ra     |da  |pa     |state|
+---------+-------+----+-------+-----+
|13823431 |2479.45|0.00|2479.45|0    |
|4035167  |2449.0 |0.00|2449.00|0    |
|4035291  |1099.42|0.00|1099.42|0    |
|4035041  |1999.0 |0.00|1999.00|0    |
|13823285 |2488.0 |0.00|2488.00|0    |
|4034219  |3449.0 |0.00|3449.00|0    |
|138230939|1649.0 |0.00|1649.00|0    |
|4035083  |7.0    |0.00|7.00   |0    |
|138230935|1299.0 |0.00|1299.00|0    |
|13823231 |499.0  |0.00|499.00 |0    |
+---------+-------+----+-------+-----+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- ra: double (nullable = true)
 |-- da: string (nullable = true)
 |-- pa: string (nullable = true)
 |-- state: integer (nullable = true)
     */

    //分组求每个用户的优惠订单数、总订单数、优惠总金额、应收总金额
    //tdon 优惠订单数
    val tdonColumn = sum('state) as "tdon"
    //ton 总订单总数
    val tonColumn = count('state) as "ton"
    //tda 优惠总金额
    val tda = sum('da) as "tda"
    //tra 应收总金额
    val traColumn = sum('ra) as "tra"
    val tempDF2: DataFrame = tempDF.groupBy('userId)
      .agg(tdonColumn, tonColumn, tda, traColumn)
    //tempDF2.show(10,false)
    //tempDF2.printSchema()
    /*
用户id  优惠订单数 总订单总数 优惠总金额 应收总金额
+---------+----+---+------+------------------+
|userId   |tdon|ton|tda   |tra               |
+---------+----+---+------+------------------+
|4033473  |3   |142|500.0 |252430.92         |
|13822725 |4   |116|800.0 |180098.34         |
|13823681 |1   |108|200.0 |169946.1          |
|138230919|3   |125|600.0 |240661.56999999998|
|13823083 |3   |132|600.0 |234124.17         |
|13823431 |2   |122|400.0 |181258.22         |
|4034923  |1   |108|200.0 |167674.89         |
|4033575  |4   |125|650.0 |255866.40000000002|
|13822841 |0   |113|0.0   |205931.91         |
|13823153 |6   |133|1200.0|251898.57         |
+---------+----+---+------+------------------+

 */
    //psm = 优惠订单占比 + 平均优惠金额占比 + 优惠总金额占比   (运营/产品提供)
    //psm = 优惠订单数/总订单数 + 平均优惠金额/平均每单应收金额 + 优惠总金额/应收总金额
    //psm = 优惠订单数/总订单数 + ((优惠总金额/优惠订单数)/(应收总金额/总订单数)) + 优惠总金额/应收总金额
    //注意:优惠订单数tdon和优惠总金额tda 有可能为0,如何处理?
    //如果在分子不用管,如果在分母怎么办?
    //注意:在SparkSQL中,分母为零,最后该条记录为null
    //如果简单处理,可以在查询结束之后过滤掉null值
    //但是需要注意:在我们这个业务中,优惠订单数tdon作为分母,是有意义的,表示该用户对优惠不敏感,所以最后不应该将该条记录过滤掉
    //那么可以将优惠订单数tdon等于0的,设置一个默认的较小的值如1 ,使用when即可
    //那么我们这里要给大家演示SparkSQL中null的比较方法,应该是使用isNull和isNotNull
    //所以就直接在后面的操作过滤掉null
    val psmColumn = 'tdon/'ton  + (('tda/'tdon)/('tra/'ton)) + 'tda/'tra as "psm"
    val psmResult: DataFrame = tempDF2.select('userId, psmColumn)
        //.filter('psm =!= null) 错误写法
        .filter('psm.isNotNull)
    psmResult.show(10,false)
    psmResult.printSchema()
    /*
 +---------+-------------------+
|userId   |psm                |
+---------+-------------------+
|4033473  |0.11686252330855691|
|13822725 |0.16774328728519597|
|13823681 |0.13753522440350205|
|138230919|0.1303734438365045 |
|13823083 |0.1380506927739941 |
|13823431 |0.15321482374431458|
|4034923  |0.13927276336831218|
|4033575  |0.11392752155030905|
|13823153 |0.15547466292943982|
|4034191  |0.11026694172505715|
+---------+-------------------+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- psm: double (nullable = true)
     */
    //2.特征工程-特征向量化
    val vectorDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("psm"))
      .setOutputCol("vectorFeatures")
      .transform(psmResult)
    vectorDF.show(10,false)
    vectorDF.printSchema()

    //3.创建模型并训练-K值选取-模型保存和加载
    //K值选取
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

    //模型保存和加载
    val path:String = "/model/PSM"
    var model:KMeansModel = null
    if(HDFSUtils.getInstance().exists(path)){
      //目录存在,不是第一次,应该直接加载该模型
      println("目录存在,不是第一次,应该直接加载该模型")
      model = KMeansModel.load(path)
    }else{
      //目录不存在,是第一次,需要创建模型并训练,最后将模型保存到path
      println("目录不存在,是第一次,需要创建模型并训练,最后将模型保存到path")
      model = new KMeans()
        .setK(5) //设置K
        .setSeed(10) //随机种子
        .setMaxIter(10) //最大迭代次数
        .setFeaturesCol("vectorFeatures")
        .setPredictionCol("predict")
        .fit(vectorDF)
      model.save(path)
    }

    //4.使用模型进行预测(聚类)
    val result: DataFrame = model.transform(vectorDF)

    //5.拉链
    val predictAndTagsId: Map[Int, Long] = zip(fiveDF,model)

    //6.打标签
    val predict2tagsId = udf((predict: Int) => {
      predictAndTagsId(predict)
    })
    val newDF: DataFrame = result.select('userId, predict2tagsId('predict).as("tagsId"))
    newDF.show(10, false)

    null
  }
}
