package net.sansa_stack.examples.spark.owl
import org.apache.spark.rdd.RDD;

object FormingRules
{
      private val tD = Preprocessing.tableDetail
      private val tNameStr = Preprocessing.tableNameStr
      
      def run(rddFit: RDD[(List[String], Double, Double)])
      {
            if(Preprocessing.flag)
            {
                 formingRules(dropDuplicate(rddFit))
            }
            else
            {//dropDuplicateForTwoAtoms(rddFit)
                 formingRulesForTwoAtoms(dropDuplicateForTwoAtoms(rddFit))                  
            }
      }
      def dropDuplicate(rddFit: RDD[(List[String], Double, Double)]):  RDD[(String, String, String, Double, Double)] =
      {
           val dropDupl = rddFit.map(f=>
                  {
                        val head = tNameStr(f._1(0))+"("+tD(f._1(0)).head+","+tD(f._1(0)).last+")"
                        val body1 = tNameStr(f._1(1))+"("+tD(f._1(1)).head+","+tD(f._1(1)).last+")"
                        val body2=tNameStr(f._1(2))+"("+tD(f._1(2)).head+","+tD(f._1(2)).last+")"
                        val conf = f._2
                        val fit = f._3
                        val key = (head,conf,fit)
                        (key ->Iterable((body1,body2)))
                  }).reduceByKey(_++_).map(
                         f=>{
                               val k = f._1
                               val unique = f._2.toList.map{case (a, b) => if(a > b) (a, b) else (b, a)}.distinct
                          (k,unique)
             }).flatMapValues(f=>f).map(f=>{
                   (f._1._1,f._2._1,f._2._2,f._1._2,f._1._3)
             })
             
            dropDupl
      }
      def dropDuplicateForTwoAtoms(rddFit: RDD[(List[String], Double, Double)]):  RDD[(String, String, Double, Double)] =
      {
           val dropDupl = rddFit.map(f=>
                  {
                        val head = tNameStr(f._1(0))+"("+tD(f._1(0)).head+","+tD(f._1(0)).last+")"
                        val body1 = tNameStr(f._1(1))+"("+tD(f._1(1)).head+","+tD(f._1(1)).last+")"
                        val conf = f._2
                        val fit = f._3
                        val key = (head,conf,fit)
                        (key->body1)
                  }).groupByKey().flatMapValues(f=>f).map(f=>{
                   (f._1._1,f._2,f._1._2,f._1._3)})/*.reduceByKey(_+_).map(f=>{
                   (f._1._1,f._2,f._1._2,f._1._3)
             })*/
            
                  /*.groupByKey().flatMapValues(f=>f).map(f=>{
                   (f._1._1,f._2,f._1._2,f._1._3)
             })*/
             dropDupl
      }
      def formingRulesForTwoAtoms(rddFit: RDD[(String, String, Double, Double)])
      {            
            rddFit.foreach(f=>
                  {
                        println(f._1+" <= "+f._2+" "+f._3+" "+f._4)
                  })
      }
      def formingRules(rddFit: RDD[(String, String, String, Double, Double)])
      {
            rddFit.foreach(f=>
                  {
                        println(f._1+" <= "+f._2+" ^ "+f._3+" "+f._4+" "+f._5)
                  })
      }
      
}