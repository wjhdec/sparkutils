package pers.wjh.bigata.utils.core.union

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.Union


/**
  * 多个dataset union
  */
case class MultiUnion(@transient spark:SparkSession) {
  /**
    * 递归方式union
    * @param dsArray 输入array
    * @return
    */
  @deprecated("方法比较直接，仅作展示","wjh")
  def recurUnionGroup(dsArray: Seq[Dataset[Row]]):Dataset[Row]= recurUnionGroup(dsArray,0)

  /**
    * 递归union执行
    * @param dsArray 要union的列表
    * @param index 位置
    * @return
    */
  private def recurUnionGroup(dsArray: Seq[Dataset[Row]], index:Int):Dataset[Row]=
    if (index < dsArray.length-1) dsArray(index).union(recurUnionGroup(dsArray,index+1)) else dsArray.last

  /**
    * 调用Union方法
    * @param dsArray
    * @return
    */
  def unionGroup(dsArray: Seq[Dataset[Row]]):Dataset[Row] =
    new Dataset(spark,Union(dsArray.map(_.queryExecution.logical)),RowEncoder(dsArray.head.schema))

}
