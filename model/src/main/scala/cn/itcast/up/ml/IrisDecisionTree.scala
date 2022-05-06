package cn.itcast.up.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.DoubleType

/**
 * Author itcast
 * Date 2019/12/11 16:39
 * Desc 使用决策树算法完成鸢尾花数据集分类
 */
object IrisDecisionTree {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IrisDecisionTree")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    //1. 数据读取
    val source: DataFrame = spark.read
      .csv("file:///D:\\备课\\用户画像\\资料\\数据集\\iris_tree.csv")
      .toDF("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width", "Species")
      .select(
        'Sepal_Length cast DoubleType,
        'Sepal_Width cast DoubleType,
        'Petal_Length cast DoubleType,
        'Petal_Width cast DoubleType,
        'Species)
    source.show(10,false)
    source.printSchema()
    /*
 +------------+-----------+------------+-----------+-----------+
|Sepal_Length|Sepal_Width|Petal_Length|Petal_Width|Species    |
+------------+-----------+------------+-----------+-----------+
|5.1         |3.5        |1.4         |0.2        |Iris-setosa|
|4.9         |3.0        |1.4         |0.2        |Iris-setosa|
|4.7         |3.2        |1.3         |0.2        |Iris-setosa|
|4.6         |3.1        |1.5         |0.2        |Iris-setosa|
|5.0         |3.6        |1.4         |0.2        |Iris-setosa|
|5.4         |3.9        |1.7         |0.4        |Iris-setosa|
|4.6         |3.4        |1.4         |0.3        |Iris-setosa|
|5.0         |3.4        |1.5         |0.2        |Iris-setosa|
|4.4         |2.9        |1.4         |0.2        |Iris-setosa|
|4.9         |3.1        |1.5         |0.1        |Iris-setosa|
+------------+-----------+------------+-----------+-----------+
only showing top 10 rows

root
 |-- Sepal_Length: double (nullable = true)
 |-- Sepal_Width: double (nullable = true)
 |-- Petal_Length: double (nullable = true)
 |-- Petal_Width: double (nullable = true)
 |-- Species: string (nullable = true)
     */

    //2.标签列数据值化(字符串-->索引)
    val stringIndexer: StringIndexer = new StringIndexer()
      .setInputCol("Species")
      .setOutputCol("Species_index")
    val stringIndexerModel: StringIndexerModel = stringIndexer.fit(source)

    //3.特征工程(可以进行归一化将数据缩放到0~1之间,这步我们上次KMeans中做过,这里不再重复了,这里做一下向量化即可)
    val vectorAssembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width"))
      .setOutputCol("vecotrFeatures")

    //4.构建决策树分类器
    val decisionTreeClassifier: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setFeaturesCol("vecotrFeatures") //设置特征列,需要向量化之后的
      .setLabelCol("Species_index") //设置标签列,需要数值化之后的
      .setPredictionCol("predict")//预测的结果是数字
      .setMaxDepth(5) //最大深度
      .setImpurity("gini")//设置不纯度(不确定性的衡量标准,用信息熵,用基尼系数)

    //5.还原预测列的数字为字符串
    val indexToString: IndexToString = new IndexToString()
      .setInputCol("predict")
      .setOutputCol("predict_String")
      .setLabels(stringIndexerModel.labels)//从stringIndexerModel中获取标签列

    //6.划分训练集和测试集
    val Array(testData,trainData) = source.randomSplit(Array(0.2,0.8),10)
    //val array: Array[Dataset[Row]] = source.randomSplit(Array(0.2,0.8),10)
    //val testData = array(0)
    //val trainData = array(1)

    //7.构建Pipeline
    val pipeline: Pipeline = new Pipeline().setStages(Array(stringIndexerModel,vectorAssembler,decisionTreeClassifier,indexToString))

    //8.训练
    val pmodel: PipelineModel = pipeline.fit(trainData)

    //9.预测(测试)
    val trainResult: DataFrame = pmodel.transform(trainData)
    val testResult: DataFrame = pmodel.transform(testData)
    testResult.show(10,false)

    //10.从决策数中查看决策过程
    //需要将decisionTreeClassifier转成DecisionTreeClassificationModel才可以调用toDebugString方法查看决策过程
    //需要注意:只有pipeline训练之后,里面的decisionTreeClassifier分类器才会变为DecisionTreeClassificationModel模型
    val treeModel: DecisionTreeClassificationModel = pmodel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    val treeString: String = treeModel.toDebugString
    println(treeString)

    //11.创建多分类评估器-查看精确率和错误率
    val accuracy: Double = new MulticlassClassificationEvaluator()
      .setPredictionCol("predict")
      .setLabelCol("Species_index")
      .setMetricName("accuracy")
      .evaluate(testResult)//我们想要的是模型在测试集上的表现
    println("在测试集上的精确率为:"+ accuracy)
    println("在测试集上的错误率为:"+ (1-accuracy))
    /*
    DecisionTreeClassificationModel (uid=dtc_66010268d6a4) of depth 4 with 13 nodes
  If (feature 2 <= 1.9)
   Predict: 0.0
  Else (feature 2 > 1.9)
   If (feature 3 <= 1.7)
    If (feature 2 <= 5.0)
     Predict: 1.0
    Else (feature 2 > 5.0)
     If (feature 0 <= 6.0)
      Predict: 1.0
     Else (feature 0 > 6.0)
      Predict: 2.0
   Else (feature 3 > 1.7)
    If (feature 2 <= 4.8)
     If (feature 0 <= 5.9)
      Predict: 1.0
     Else (feature 0 > 5.9)
      Predict: 2.0
    Else (feature 2 > 4.8)
     Predict: 2.0

在测试集上的精确率为:0.9090909090909091
在测试集上的错误率为:0.09090909090909094
     */

  }
}
