package net.sansa_stack.examples.spark.owl

import org.apache.spark.storage.StorageLevel
import org.semanticweb.owlapi.model._
import scala.collection.mutable
import scala.collection.mutable.{LinkedHashSet,HashMap}
import org.apache.spark.{ HashPartitioner, RangePartitioner }
import scala.collection.concurrent.TrieMap
import org.apache.spark.ml.feature.{ MinHashLSH,HashingTF }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, udf }

object Preprocessing
{
  private val sc = OWLReaderRDD.sparkContext
  private var simTh = 0.50 //Similarity Threshold
  val tableValue = new HashMap[String, Seq[(String,String)]]()
  val tableNameStr = new HashMap[String,String]()
  val vpTableSizes = new HashMap[String, Long]()
  val tableDetail = new HashMap[String, LinkedHashSet[String]]()
  val overLap = new HashMap[(String, String), LinkedHashSet[String]]() 
  var flag :Boolean = true
  var tablesToBeCreated: RDD[String] = null
  def run(spark: SparkSession, filteredRDD: RDD[OWLAxiom], propDetailRDD: RDD[OWLAxiom])
  {
    val partitionRDD= verticalPartitioning(filteredRDD)
    fetchTableName(partitionRDD)
    val predWithTriples = prepForClustering(filteredRDD)
    val domainPropRdd=propDetailRDD.filter(f=>(f.isInstanceOf[OWLObjectPropertyDomainAxiom]))
    var domainSeedRdd=prepareSeed(domainPropRdd)
    val rangePropRdd=propDetailRDD.filter(f=>(f.isInstanceOf[OWLObjectPropertyRangeAxiom]))
    var rangeSeedRdd=prepareSeed(rangePropRdd)
    val domRanSeedRdd= (domainSeedRdd.join(rangeSeedRdd)).map(f=>{
      val key = f._1
      val value = f._2._1 ++ f._2._2
      (key,value)
    }).filter(f=>f._2.size==2).filter(f=>(!f._2.exists(p=>p.contains("ObjectUnionOf"))))
    fetchTableDetail(domRanSeedRdd)
    val bcTableDetail = sc.broadcast(tableDetail)
    domainSeedRdd=domRanSeedRdd.map({ f=>
      val k = f._1
      val v = new LinkedHashSet()+=(f._2.head)
      (k,v)
      })
    rangeSeedRdd=domRanSeedRdd.map({ f=>
      val k = f._1
      val v = new LinkedHashSet() +=(f._2.last)
      (k,v)
      })
    val clusterOfDomRanSeedRdd=localSensitiveHashing(spark,predWithTriples,domRanSeedRdd)
    val clusterOfDomSeedRdd=localSensitiveHashing(spark,predWithTriples,domainSeedRdd)
    val clusterOfRanSeedRdd=localSensitiveHashing(spark,predWithTriples,rangeSeedRdd)
    val clusterofAllRDD = clusterOfDomRanSeedRdd.union(clusterOfDomSeedRdd).union(clusterOfRanSeedRdd)
    if(clusterofAllRDD.isEmpty())
      println("No Cluster formed")
    else
    {
      val comboPredAtom = comboOfPredAtom(clusterofAllRDD,true)
      val combo = comboPredAtom.map(f=>f.combinations(2)).flatMap(g=>g).distinct()
      for(c<-combo)
      {
        val common = bcTableDetail.value.getOrElseUpdate(c(0),LinkedHashSet[String]()).intersect(bcTableDetail.value.getOrElseUpdate(c(1),LinkedHashSet[String]()))
        overLap((c(0),c(1)))=common
      }
      println("inMemory 3 atoms")
      InMemoryFitness.run(comboPredAtom)
      if(clusterOfDomRanSeedRdd.isEmpty())
      {
             println("No two atoms rule formed")
      }
      else
      {
            val domRanComboPredAtom = comboOfPredAtom(clusterOfDomRanSeedRdd,false)
            flag = false
            println("inMemory 2 atoms")
            InMemoryFitness.run(domRanComboPredAtom)                        
      }
      println("SQL Output 3 atoms")
     	SparkSqlFitness.run(spark,comboPredAtom,partitionRDD,domRanSeedRdd)
     	if(clusterOfDomRanSeedRdd.isEmpty()){println("No two atoms rule formed")}
     	else
     	{
     	  val domRanComboPredAtom = comboOfPredAtom(clusterOfDomRanSeedRdd,false)
     	  flag = false
     	  println("inMemory 2 atoms")
     	  InMemoryFitness.run(domRanComboPredAtom)
     	  println("SQL Output 2 atoms")
     	  SparkSqlFitness.run(spark,domRanComboPredAtom,partitionRDD,domRanSeedRdd)
      }
    }     	
  }
  
  def verticalPartitioning(filteredRDD: RDD[org.semanticweb.owlapi.model.OWLAxiom]): RDD[(String, Iterable[(String, String)])]= 
  {
    val partitionRDD =filteredRDD.map(f=>f.toString().split(" ")).map(f => {
      val key =  f(0).substring(f(0).indexOf("(")+1)
      val sub = f(1)
      val obj = f(2).substring(0,f(2).length-1)
      (key, Iterable((sub,obj)))
      }).reduceByKey((x,y)=>((x ++ y)))
      partitionRDD
  }
  def fetchTableName(partitionRDD: RDD[(String, Iterable[(String, String)])])
  {
    for (p<-partitionRDD)
    {
          tableNameStr(p._1)=p._1.split("#")(1).dropRight(1)
          tableValue(p._1)=p._2.toSeq			
    }
    for (p<-partitionRDD) vpTableSizes(p._1)=p._2.size
  }
  def fetchTableDetail(domRanSeedRdd: RDD[(String, LinkedHashSet[String])])
  {
    for(t<-domRanSeedRdd) tableDetail(t._1)=t._2.map(f=>f.split("#")(1).dropRight(1))            
  }
  def prepareSeed(propDetailRDD: RDD[OWLAxiom]): RDD[(String, LinkedHashSet[String])]=
  {
    val seedRDD = propDetailRDD.map(f=>f.toString().split(" ")).map(f => {
      val key =  f(0).substring(f(0).indexOf("(")+1)
      val value = f(1).substring(0,f(1).length-1)
      (key, value)})    
    val initialSet1 = mutable.LinkedHashSet.empty[String]
    val addToSet1 = (s: mutable.LinkedHashSet[String], v: String) => s += v
    val mergePartitionSets1 = (p1: mutable.LinkedHashSet[String], p2: mutable.LinkedHashSet[String]) => p1 ++= p2
    val uniqueByPred = seedRDD.aggregateByKey(initialSet1)(addToSet1, mergePartitionSets1)   
    uniqueByPred            
  }
  def localSensitiveHashing(spark: SparkSession,predWithTriples:RDD[(String,scala.collection.mutable.Set[(String, String, String)])],uniqueByPred: RDD[(String, LinkedHashSet[String])]): RDD[List[String]]=
  {
    import spark.implicits._
    val hashtoseq = uniqueByPred.map(f => (f._1, f._2.toSeq))
    val part = new RangePartitioner(8, hashtoseq)
    val partitioned = hashtoseq.partitionBy(part).persist(StorageLevel.MEMORY_AND_DISK)
    val dfA = partitioned.toDF("id", "values")
    
    val hashingTF = new HashingTF().setInputCol("values").setOutputCol("features").setNumFeatures(20)

    val featurizedData = hashingTF.transform(dfA)
    
    val minhash = new MinHashLSH().setNumHashTables(2).setInputCol("features").setOutputCol("hashes")

    val model = minhash.fit(featurizedData)
   
    val dffilter = model.approxSimilarityJoin(featurizedData, featurizedData, simTh)
    
    val result = dffilter.filter($"datasetA.id".isNotNull).filter($"datasetB.id".isNotNull).filter(($"datasetA.id" =!= $"datasetB.id"))
    .select(col("datasetA.id").alias("id1"),col("datasetB.id").alias("id2"))

    val resultRow = result.repartition(400).persist(StorageLevel.MEMORY_AND_DISK)
    
    val resultRDD = resultRow.rdd.map(row => {
      val id = row.getString(0)
      val value = row.getString(1)
      (id, value)
    })
    
    val initialSet3 = mutable.Set.empty[String]
    val addToSet3 = (s: mutable.Set[String], v: String) => s += v
    val mergePartitionSets3 = (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2
    val uniqueByKey3 = resultRDD.aggregateByKey(initialSet3)(addToSet3, mergePartitionSets3)

    val k = uniqueByKey3.map(f => ((f._1, (f._2 += (f._1)).toSet)))
    
    val partitioner = new HashPartitioner(500)
    val mapSubWithTriplesPart = predWithTriples.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)

    val ys = k.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)
    val joinSimSubTriples2 = ys.join(mapSubWithTriplesPart)

    val clusterOfPred = joinSimSubTriples2.map({
      case (s, (iter, iter1)) => ((iter).toSet, iter1)
    })
    
    val initialSet = mutable.HashSet.empty[mutable.Set[(String, String, String)]]
    val addToSet = (s: mutable.HashSet[mutable.Set[(String, String, String)]], v: mutable.Set[(String, String, String)]) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[mutable.Set[(String, String, String)]], p2: mutable.HashSet[mutable.Set[(String, String, String)]]) => p1 ++= p2
    val uniqueByKey = clusterOfPred.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
   
    val propCluster = uniqueByKey.map({f=>f._2.toList}).map(f=>f.flatten).filter(f=>f.nonEmpty)
    
    val simiPred =propCluster.map(f=>f.map(f=>f._2).distinct).filter(f=>f.length>2)
    mapSubWithTriplesPart.unpersist()
    ys.unpersist()    
    resultRow.unpersist()
    partitioned.unpersist()
    
    simiPred
  }
  def prepForClustering(filteredRDD: RDD[org.semanticweb.owlapi.model.OWLAxiom]): RDD[(String,scala.collection.mutable.Set[(String,String, String)])]=
  {
    val tempRDD =filteredRDD.map(f=>f.toString().split(" "))
        .map(f => (f(0).substring(f(0).indexOf("(")+1),
                    (f(1),f(0).substring(f(0).indexOf("(")+1),f(2).substring(0,f(2).length-1))))
                    
    val initialSet = mutable.Set.empty[(String,String, String)]
    val addToSet = (s: mutable.Set[(String,String, String)], v: (String,String, String)) => s += v
    val mergePartitionSets = (p1: mutable.Set[(String,String, String)], p2: mutable.Set[(String,String, String)]) => p1 ++= p2
    val setOfTriples = tempRDD.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    
    setOfTriples        
  }
  def comboOfPredAtom(clusterofAllRDD: RDD[List[String]], flag:Boolean): RDD[List[String]]=
  {
    if(flag)
      return (clusterofAllRDD.flatMap(f=>f.combinations(3)).flatMap(f=>f.permutations))
    else
      return (clusterofAllRDD.flatMap(f=>f.combinations(2)).flatMap(f=>f.permutations))
  }
}
