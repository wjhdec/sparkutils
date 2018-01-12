package pers.wjh.bigata.utils.core.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test
import pers.wjh.bigata.utils.core.union.MultiUnion

class UnionTest {
  private val conf = new SparkConf()
  conf.setMaster("local")
  private val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._
  private val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  private val ds1 = sc.parallelize(Seq((1,"a"),(2,"b"))).toDF("id","value")
  private val ds2 = sc.parallelize(Seq((3,"c"),(4,"d"))).toDF("id","value")
  private val ds3 = sc.parallelize(Seq((5,"e"),(6,"f"))).toDF("id","value")

  @Test
  def testRecurUnion():Unit={
    println("递归union结果：")
    val dsList = Seq(ds1,ds2,ds3)
    val multiUnion = MultiUnion(spark)
    multiUnion.recurUnionGroup(dsList).show()
  }
  @Test
  def testBase():Unit={
    println("依次union结果：")
    val dsAll = ds1.union(ds2).union(ds3)
    dsAll.show()
  }

  @Test
  def testUnion():Unit={
    println("调用union类结果：")
    val dsList = Seq(ds1,ds2,ds3)
    val multiUnion = MultiUnion(spark)
    multiUnion.unionGroup(dsList).show()
  }
}
