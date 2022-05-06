package cn.itcast.up.statistics

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql._

/**
 * Author itcast
 * Date 2019/12/7 9:23
 * Desc 完成统计型标签-年龄段标签/模型的开发
 */
object AgeModel2 extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }
  /**
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @return
   */
  override def getTagId(): Long = 14

  /**
   * 根据5级标签和HBase数据进行标签计算(每个模型/标签计算方式不同)
   * 该抽象方法应该由子类/实现类提供具体的实现/扩展
   * @param fiveDF
   * @param hbaseDF
   * @return
   */
  override def compute(fiveDF: DataFrame, hbaseDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //5.根据fiveDF和hbaseDF计算结果-newDF
    //5.1统一格式,将birthday:1992-05-31转为19920531
    val birthdayColumn: Column = regexp_replace('birthday, "-", "")
    val tempDF: DataFrame = hbaseDF.select('id, birthdayColumn.as("birthday"))
    tempDF.show(10, false)
    //5.2切割列,一列边两列,将fiveDF中的rule列:19500101-19591231,变为start:19500101和end:19591231
    val rangDF: DataFrame = fiveDF.as[(Long, String)].map(t => {
      val arr: Array[String] = t._2.split("-")
      (t._1, arr(0), arr(1))
    }).toDF("tagsId", "start", "end")
    rangDF.show(10, false)
    //5.3比较大小tempDF.birthday.between(start,end)
    val newDF: DataFrame = tempDF.join(rangDF)
      .where(tempDF.col("birthday").between(rangDF.col("start"), rangDF.col("end")))
      .select(tempDF.col("id").as("userId"), rangDF.col("tagsId"))
    newDF.show(10, false)
    newDF
  }
}
