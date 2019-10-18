package net.sansa_stack.examples.spark.owl

import org.apache.spark.rdd.RDD
import scala.collection.mutable.{LinkedHashSet,HashMap}
import scala.math.BigDecimal

object InMemoryFitness
{
  private val tableVal = Preprocessing.tableValue
  private val tD = Preprocessing.tableDetail
  private val oL = Preprocessing.overLap
  private val tableSize =Preprocessing.vpTableSizes
  private val minConf = 0.1
  private val minHC = 0.01
  
  def run(comboPredAtom: RDD[List[String]])
  {
    val rddFit = comboEval(comboPredAtom)
    FormingRules.run(rddFit)
  }
  def comboEval(comboPredAtom: RDD[List[String]]): RDD[(List[String], Double, Double)]=
  {
    var commont2t3,commont1t2,commont1t3: LinkedHashSet[String] = LinkedHashSet()
    var row1,row2,row3,supp,confDeno,common,val1,val2,val3: Set[String] = Set()
    var suppVal, headDeno,confDenoVal : Long = 0
    var so1,so2,so3:Seq[(String, String)] = null
    if(Preprocessing.flag)
    {
      val d = for(l<-comboPredAtom) yield
      {
        var head1 = tD(l(0))
        var tup = (head1.head,head1.last)
        so1 = tableVal(l(0)) :+ tup
        head1 = tD(l(1))
        tup = (head1.head,head1.last)
        so2 = tableVal(l(1)) :+ tup
        head1 = tD(l(2))
        tup = (head1.head,head1.last)
        so3 = tableVal(l(2)) :+ tup
        commont2t3 = oL(l(1),l(2))
        commont1t2 = oL(l(0),l(1))
        commont1t3= oL(l(0),l(2))
        if((commont1t2.size.equals(1))&&(commont2t3.size.equals(1))&&(commont1t3.size.equals(1)))
        {
          row1 = findRow(so1,commont1t2.head) 
          row2 = findRow(so2,commont2t3.head) 
          row3 = findRow(so3,commont1t3.head)
          supp = (row1.intersect(row2).intersect(row3)) - commont1t2.head
          confDeno = row2.intersect(row3) - commont1t2.head
        }
        else if((commont1t2.size.equals(2))&&(commont2t3.size.equals(2))&&(commont1t3.size.equals(2)))
        {
          val joint1t2 = commont1t2.head
          commont1t2 = commont1t2 - joint1t2
          commont2t3 = commont2t3 - joint1t2
          commont1t3 = commont1t3 - joint1t2     
          row1 = findRow(so1,commont1t2.head)
          row2 = findRow(so2,commont2t3.head)
          row3 = findRow(so3,commont1t3.head)
          common = (row1.intersect(row2).intersect(row3)) - commont2t3.head
          confDeno = row2.intersect(row3) - commont1t2.head
          val1 = findTuple(so1,commont1t2.head,common)
          val2 = findTuple(so2,commont2t3.head,common)
          supp = (val1.intersect(val2)) -joint1t2
        }
        else
        {
          val commonRow = (commont1t2.intersect(commont2t3).intersect(commont1t3))
          if(commonRow.nonEmpty)
          {
            row1 = findRow(so1,commonRow.head)
            row2 = findRow(so2,commonRow.head)
            row3 = findRow(so3,commonRow.head)
            confDeno = row2.intersect(row3) - commonRow.head
            common = (row1.intersect(row2).intersect(row3))- commonRow.head
            if(commont1t2.size.equals(2))
            {
              commont1t2 = commont1t2 - commonRow.head
              val1 = findTuple(so1,commonRow.head,common)
              val2 = findTuple(so2,commonRow.head,common)   
              supp = val1.intersect(val2) - commont1t2.head
            }
            else if(commont2t3.size.equals(2))
            {
              commont2t3 = commont2t3 - commonRow.head
              val2 = findTuple(so2,commonRow.head,common)
              val3 = findTuple(so3,commonRow.head,common)   
              supp = val2.intersect(val3) - commont2t3.head
              supp = common
            }
            else
            {
              commont1t3 = commont1t3 - commonRow.head
              val1 = findTuple(so1,commonRow.head,common)
              val3 = findTuple(so3,commonRow.head,common)   
              supp = val1.intersect(val3) - commont1t3.head
            }
          }               
        }
        val suppVal = if(supp.size==0) 0 else supp.size
        val confDenoVal = if(confDeno.size==0) 1 else confDeno.size
        val confVal = BigDecimal((suppVal.toDouble)/confDenoVal.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        val hcVal = BigDecimal((suppVal.toDouble)/tableSize(l(0)).toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        (l,confVal,hcVal)
      }
      return( d.filter(p=>p._2>=(minConf)).filter(p=>p._3>=(minHC)))
    }
    else
    {
      var commonRow : String = ""
      val d = for(l<-comboPredAtom) yield
      {
        var head1 = tD(l(0)) 
        var tup = (head1.head,head1.last)
        so1 = tableVal(l(0)) :+ tup
        head1 = tD(l(1))
        tup = (head1.head,head1.last)
        so2 = tableVal(l(1)) :+ tup
        confDenoVal = tableSize(l(1))
        commont1t2 = oL(l(0),l(1))
        commonRow = commont1t2.head
        commont1t2 = commont1t2 - commonRow
        row1 = findRow(so1,commonRow)
        row2 = findRow(so2,commonRow)
        common = row1.intersect(row2) - commonRow
        val1 = findTuple(so1,commonRow,common)
        val2 = findTuple(so2,commonRow,common)
        supp = val1.intersect(val2)
        val suppVal = if(supp.size==0) 0 else supp.size
        val confVal = BigDecimal((suppVal.toDouble)/confDenoVal.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        val hcVal = BigDecimal((suppVal.toDouble)/tableSize(l(0)).toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        (l,confVal,hcVal)
      }
      return( d.filter(p=>p._2>=(minConf)).filter(p=>p._3>=(minHC)))
    }
  }
  def findRow(soTable: Seq[(String, String)], common: String): Set[String]=
  {
    if(soTable.toMap.values.exists(_ == common))
    {
      return soTable.toMap.values.toSet
    }
    else
    {
      return soTable.toMap.keySet.toSet
    }
  }
  def findTuple(soTable: Seq[(String, String)], common: String, commonVal: Set[String]): Set[String]=
  {
    if(soTable.toMap.values.exists(_ == common))
    {
      return soTable.toMap.filter(f=>commonVal.contains(f._2)).keySet
    }
    else
    {
      return soTable.toMap.filter(f=>commonVal.contains(f._1)).valuesIterator.toSet
    }
   }
}