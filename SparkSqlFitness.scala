package net.sansa_stack.examples.spark.owl


import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import scala.collection.mutable.LinkedHashSet
import scala.math.BigDecimal

object SparkSqlFitness{
  private val sc = OWLReaderRDD.sparkContext
  private val sqlContext = OWLReaderRDD.sqlContext
  private val tableSizes = Preprocessing.vpTableSizes
  private val tD = Preprocessing.tableDetail
  private val overlap = Preprocessing.overLap
  private val tNameStr = Preprocessing.tableNameStr
  private val tVal = Preprocessing.tableValue
  private val minConf = 0.1
  private val minHC = 0.01
  def run(spark: SparkSession,comboPredAtom: RDD[List[String]],partitionRDD: RDD[(String, Iterable[(String, String)])],domRanSeedRdd: RDD[(String, LinkedHashSet[String])])
  {
    val rddFit = constTable(spark,partitionRDD,comboPredAtom,domRanSeedRdd)
    FormingRules.run(rddFit)
  }
  def constTable(spark: SparkSession,partitionRDD: RDD[(String, Iterable[(String, String)])],comboPredAtom: RDD[List[String]],domRanSeedRdd: RDD[(String, LinkedHashSet[String])]): RDD[(List[String], Double, Double)]=
  {
    import spark.implicits._
    var qryRdd: Array[(List[String], String, Long, String)] = null
    var qryRddForTwoAtoms: Array[(List[String], String, Long, Long)] = null
    var rddFitEval: Array[(List[String], Double, Double)] = null
    val sod = partitionRDD.join(domRanSeedRdd).map(f=>(f._1,f._2._1.toSeq,f._2._2.toList)).collect()
    val vpPath = "hdfs://172.18.160.17:54310/MayuriMendke/VP_"+OWLReaderRDD.fileName+"/"
    if(testDirExist(spark, vpPath))
      loadDF(spark,sod,vpPath)
    else
      createDF(spark,sod,vpPath)
    if(Preprocessing.flag)
    {
      qryRdd = formQry(comboPredAtom,vpPath).collect()
      rddFitEval = for(q<- qryRdd)yield
      {
            val suppVal = if(q._2.nonEmpty) sqlContext.sql(q._2).count() else 0
            val confDenoVal = if(q._4.nonEmpty) sqlContext.sql(q._4).count() else 0
            val fitness = if (q._3 == 0) 0 else BigDecimal(suppVal.toDouble/q._3.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
            val confVal = if (confDenoVal == 0) 0 else BigDecimal(suppVal.toDouble/confDenoVal.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
            (q._1,confVal,fitness)
      }
    }
    else
    {
      qryRddForTwoAtoms = formQryForTwoAtoms(comboPredAtom).collect()
      rddFitEval = for(q<- qryRddForTwoAtoms)yield
      {
        val suppVal = if(q._2.nonEmpty) sqlContext.sql(q._2).count() else 0
        val fitness = if (q._3 == 0) 0 else BigDecimal(suppVal.toDouble/q._3.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        val confVal = if (q._4 == 0) 0 else BigDecimal(suppVal.toDouble/q._4.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        (q._1,confVal,fitness)
      }
    }
    val rddFit =sc.parallelize(rddFitEval.filter(p=>p._2>=(minConf)).filter(p=>p._3>=(minHC)))
    rddFit
  }
  def formQryForTwoAtoms(comboPredAtom: RDD[List[String]]):RDD[(List[String], String, Long, Long)]=
  {
    val qry = for(c<-comboPredAtom) yield
    {
      var suppQry: String = ""
      var commont1t2= overlap(c(0),c(1))
      val table1 = tNameStr(c(0))
      val table2 = tNameStr(c(1))
      val hcDeno = tableSizes(c(0))
      val confDeno = tableSizes(c(1))
      if(commont1t2.size.equals(2))
      {
        val joint1t2 = commont1t2.head
        commont1t2 = commont1t2 - joint1t2
        suppQry ="Select distinct(*) from "+table1+" as t1,"+table2+" as t2 where "
        suppQry = suppQry +"t1."+commont1t2.head+" = t2."+commont1t2.head
        suppQry = suppQry +" and t1."+joint1t2+" = t2."+joint1t2
      }
      else
        suppQry = ""
     (c,suppQry,hcDeno,confDeno)   
    }
    qry
   }
  def formQry(comboPredAtom: RDD[List[String]],vpPath: String):RDD[(List[String], String, Long, String)]=
  {
    val qry= for(c<-comboPredAtom)yield
    {
      var confDenoQry,suppQry: String = ""
      var commont2t3= overlap(c(1),c(2))
      var commont1t2= overlap(c(0),c(1))
      var commont1t3= overlap(c(0),c(2))
      val table1 = tNameStr(c(0))
      val table2 = tNameStr(c(1))
      val table3 = tNameStr(c(2))
      val hcDeno = tableSizes(c(0))
      if((commont1t2.size.equals(2))&&(commont1t3.size.equals(2)))
      {
        val joint1t2 = commont1t2.head
        commont1t2 = commont1t2 - joint1t2
        commont2t3 = commont2t3 - joint1t2
        commont1t3 = commont1t3 - joint1t2
        suppQry ="Select distinct(*) from "+table1+" as t1,"+table2+" as t2,"+table3+" as t3 where "
        suppQry = suppQry +"t1."+commont1t2.head+" = t2."+commont1t2.head
        suppQry = suppQry +" and t2."+commont2t3.head+" = t3."+commont2t3.head
        suppQry = suppQry +" and t1."+commont1t3.head+" = t3."+commont1t3.head
        suppQry = suppQry +" and t1."+joint1t2+" = t2."+joint1t2
      }
      else
      {
        suppQry ="Select distinct(*) from "+table1+" as t1,"+table2+" as t2,"+table3+" as t3 where "
        if(commont1t2.nonEmpty)
        {
          if(commont1t2.size.equals(2))
          {
            val joint1t2 = commont1t2.last
            commont1t2 = commont1t2 - joint1t2
            suppQry = suppQry +"t1."+commont1t2.head+" = t2."+commont1t2.head
            suppQry = suppQry +" and t1."+joint1t2+" = t2."+joint1t2
          }
          else
          {
            suppQry = suppQry +"t1."+commont1t2.head+" = t2."+commont1t2.head
          }
        }
        if(commont1t3.nonEmpty)
        {
          if(commont1t3.size.equals(2))
          {
            val joint1t3 = commont1t3.last
            commont1t3 = commont1t3 - joint1t3
            if(commont1t2.nonEmpty)
            {
              suppQry = suppQry +" and t1."+commont1t3.head+" = t3."+commont1t3.head
            }
            else
            {
              suppQry = suppQry +"t1."+commont1t3.head+" = t3."+commont1t3.head
            }
            suppQry = suppQry +" and t1."+joint1t3+" = t3."+joint1t3+" "
          }
          else
          {
            if(commont1t2.nonEmpty)
            {
              suppQry = suppQry +" and t1."+commont1t3.head+" = t3."+commont1t3.head
            }
            else
            {
              suppQry = suppQry +"t1."+commont1t3.head+" = t3."+commont1t3.head
            } 
          }
        }
        if(commont2t3.nonEmpty)
        {
          if(commont1t2.nonEmpty && commont1t3.nonEmpty)
          {
            suppQry = suppQry +" and t2."+commont2t3.head+" = t3."+commont2t3.head
          }
          else
          {
            suppQry = ""
          }
        }
      }
      if(commont2t3.nonEmpty)
        confDenoQry="Select distinct(*) from "+table2+" as t2,"+table3+" as t3 where t2."+commont2t3.head+" == t3."+commont2t3.head
     (c,suppQry,hcDeno,confDenoQry)
    }
    qry
   }
  def testDirExist(spark: SparkSession,path: String): Boolean = 
  {
    val hadoopfs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
    val p = new Path(path)
    hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
  }
  def createDF(spark: SparkSession,sod: Array[(String, Seq[(String, String)], List[String])], path: String)
  {
    import spark.implicits._
    for(k<-sod)
    {
      val s = k._3(0).split("#")(1).dropRight(1)
      val o = k._3(1).split("#")(1).dropRight(1)
      val so = k._2.toDF(s,o)
      val tableName =tNameStr(k._1)
      so.createOrReplaceTempView(tableName)
      sqlContext.cacheTable(tableName)
      so.write.parquet(path+tableName+".parquet")
    }
  }
  def loadDF(spark: SparkSession, h: Array[(String, Seq[(String, String)], List[String])],path: String)
  {
    for (k <- h)
    {
      val tableName = tNameStr(k._1)  
      var so = sqlContext.read.parquet(path+tableName+ ".parquet")          
      so.createOrReplaceTempView(tableName)
      sqlContext.cacheTable(tableName)
    }
   }
}
