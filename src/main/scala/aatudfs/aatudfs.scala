package aatudfs
import org.apache.spark.sql.api.java._

import scala.collection.mutable
import scala.math.abs
import scala.math.round
import scala.math.pow
import scala.math.min
import scala.math.max

object aatAlgo{
  def lcs[A](db: Seq[A],query: Seq[A]): Int = {
    val dbSize : Int = db.size
    val querySize : Int = query.size
    val dist:(A,A) => Int =  (x:A,y:A) => if (x == y) 1 else 0
    val cache_i_J:mutable.ArrayBuffer[Int] = mutable.ArrayBuffer(dist(db.head,query.head ))
    var cache_i_j1:Int = 1073741824
    var cache_i_j1_temp:Int = 1073741824
    var curdist:Int = 0
    for (j <- 1 until dbSize){
      cache_i_J += dist(query.head,db(j))
    }

    for (i <- 1 until querySize){
      for (j <- 0 until dbSize) {
        curdist = dist(query(i),db(j))
        if (j == 0){
          if (curdist == 1){
            cache_i_j1 = curdist
          }else{
            cache_i_j1 = cache_i_J(j)
          }
        }else if (curdist == 1){
          cache_i_j1_temp = cache_i_J(j-1) + curdist
          cache_i_J(j-1) = cache_i_j1
          cache_i_j1 = cache_i_j1_temp

        }else{
          if (cache_i_j1 > cache_i_J(j)){
            cache_i_j1_temp = cache_i_j1
          }else{
            cache_i_j1_temp = cache_i_J(j)
          }
          cache_i_J(j-1) = cache_i_j1
          cache_i_j1 = cache_i_j1_temp
        }
      }
      cache_i_J(dbSize-1) = cache_i_j1
    }
    cache_i_J(dbSize-1)
  }


  def lmv(seq: Seq[Double]): collection.mutable.Map[String,Double] = {
    val roundAt1:Double=>Double = (x:Double) => (math rint x * 10) / 10
    val res = collection.mutable.Map[String,Double]()
    val minLen: Int = 2
    val maxVar: Double = 1000000.0
    val seqSize: Int = seq.size
    var x_sq_sum: Double = 0.0
    var x_sum: Double = 0.0
    var subLen: Int = 0
    var e_i: Double = 0.0
    var candi: Double = 0.0
    val minVar: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer.fill(seqSize + 1)(maxVar)
    val e_arr: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer.fill(seqSize + 1)(maxVar)
    var subArr:Seq[Double] = Nil
    for (i <- 0 until (seqSize -(minLen-1)) ) {
      for (j <- (i+minLen-1) until seqSize) {
        subLen = (j - i)+1
        if (subLen == minLen) {
          subArr = 0.0 +: seq.slice(i, j + 1)
          x_sq_sum = subArr.reduce((x, y) => x + pow(y, 2))
          x_sum = subArr.sum
          e_i = x_sum / subLen
          candi = roundAt1((x_sq_sum / subLen) - pow(e_i, 2))
          if (candi < minVar(subLen)) {
            minVar(subLen) = candi
            e_arr(subLen) = e_i
          }
        } else {
          x_sq_sum = x_sq_sum + pow(seq(j), 2)
          x_sum = x_sum + seq(j)
          e_i = x_sum / subLen
          candi = roundAt1((x_sq_sum / subLen) - pow(e_i, 2))
          if (candi < minVar(subLen)) {
            minVar(subLen) = candi
            e_arr(subLen) = e_i
          }
        }
      }
    }
    var minVar_val: Double = maxVar
    if (seqSize >= 5) {
      val minVar_under5 = minVar.slice(0, 5).min
      val minVar_over5 = minVar.slice(5, minVar.size).min
      if (minVar_over5 <= 0.5) {
        minVar_val = minVar_over5
      } else {
        minVar_val = min(minVar_under5, minVar_over5)
      }
    } else {
      minVar_val = minVar.min
    }
    def getConsecution(minVar2: mutable.ArrayBuffer[Double], minVar_val2: Double): Int = {
      for (i <- minVar2.size - 1 to 0 by -1) {
        if (abs(minVar2(i) - minVar_val2) < 0.01) {
          return i
        }
      }
      0
    }
    val consecution:Int = getConsecution(minVar,minVar_val)
    val timeInterval:Double = e_arr(consecution)
    val duration:Double = timeInterval*consecution
    res += "var" -> minVar_val
    res += "consecution" -> consecution.asInstanceOf[Double]
    res += "timeInterval" -> roundAt1(timeInterval)
    res += "duration" -> roundAt1(duration)
    res
  }


  def checkSeqSlice(seqSize:Int,from:Int,to:Int):Boolean = {
    if (from < 0)
      false
    else if (to > seqSize){
      false
    }else{
      true
    }

  }

  def arraySlice[A](array: Seq[A],from:Int,to:Int): Seq[A] ={
    val arrsize:Int = array.size
    array.slice(max(0,from),min(to,arrsize))
  }

  def dtw[A](db: Seq[A],query: Seq[A])(dist:(A,A) => Double): Double = {
    val dbSize : Int = db.size
    val querySize : Int = query.size
    val cache_i_J:mutable.ArrayBuffer[Double] = mutable.ArrayBuffer(dist(db.head,query.head))
    var cache_i_j1:Double = 1073741824.0
    var cache_i_j1_temp: Double = 1073741824.0
    var curdist:Double = 0.0
    var candis:List[Double] = Nil
    for (j <- 1 until dbSize){
      cache_i_J +=  cache_i_J(j - 1) + dist(query.head,db(j))
    }
    /*
    cache_i_j1 == cache[i][j-1] left
    cache_i_J(j) == cache[i-1][j] up
    cache_i_J(j-1) == cache[i-1][j-1] left up
    */
    for (i <- 1 until querySize){
      for (j <- 0 until dbSize) {
        curdist = dist(query(i),db(j))
        if (j == 0){
          cache_i_j1 = curdist + cache_i_J(j)
        }else{
          candis  = List(cache_i_J(j) + curdist,cache_i_J(j-1) + curdist)
          cache_i_j1_temp = cache_i_j1 + curdist
          for (idx <-candis.indices){
            cache_i_j1_temp  = min(cache_i_j1_temp,candis(idx))
          }
          cache_i_J(j-1) = cache_i_j1
          cache_i_j1 = cache_i_j1_temp
        }
      }
      cache_i_J(dbSize-1) = cache_i_j1
    }
    cache_i_J(dbSize-1)
  }

  def yw_dtw[A](db: Seq[A],query: Seq[A])(dist:(A,A) => Double): Double  = {
    import scala.collection.mutable
    val dbSize : Int = db.size
    val querySize : Int = query.size
    val cache_i_J:mutable.ArrayBuffer[Double] = mutable.ArrayBuffer(dist(db.head,query.head))
    var cache_i_j1:Double = 1073741824.0
    var cache_i_j1_temp:Double = 1073741824.0
    var disti:Double = 0.0
    var curdist:Double = 0.0
    var cand1: Double = 0.0
    for (j <- 1 until dbSize){
      disti = dist(query.head,db(j))
      if (cache_i_J(j-1) < disti)
      {
        cache_i_J +=  cache_i_J(j - 1)
      }
      else
      {
        cache_i_J += disti
      }
    }

    for (i <- 1 until querySize){
      // db는 무조건 query 보다 커야함
      for (j <- i until dbSize) {
        curdist = dist(query(i),db(j))
        if (i == j) {
          cache_i_j1_temp = curdist + cache_i_J(j-1)
          cache_i_J(j-1) = cache_i_j1
          cache_i_j1 = cache_i_j1_temp
        }
        else{
          cand1 = cache_i_j1 - dist(query(i), db(j-1))
          if (cache_i_J(j-1) < cand1 ){
            cache_i_j1_temp = curdist + cache_i_J(j-1)
          }else{
            cache_i_j1_temp = curdist + cand1
          }
          cache_i_J(j-1) = cache_i_j1
          cache_i_j1 = cache_i_j1_temp
        }
      }
      cache_i_J(dbSize-1) = cache_i_j1
    }
    var minVal :Double = cache_i_J(querySize-1)
    for (i <- querySize until dbSize ){
      if (cache_i_J(i) < minVal ){
        minVal = cache_i_J(i)
      }
    }
    minVal
  }

} //aatAlgo End

object aatGraphModules {
  def createGraphFromEdgeStringList(edgeList:Seq[String],delemeter:String):Map[String,Set[String]] = {
    var graph = Map[String, Set[String] ]()
    for ( i <- edgeList.indices){
      val fromto:Seq[String] =  edgeList(i).split(delemeter)
      if (graph.get(fromto(0)) == None){
        if (fromto(0) != fromto(1)){
          graph = graph.updated(fromto(0),Set(fromto(1)))
        }else{
          graph = graph.updated(fromto(0),Set())
        }
      }else{
        if (fromto(0) != fromto(1)){
          graph = graph.updated(fromto(0),graph.getOrElse(fromto(0),Set() ) + fromto(1))
        }
      }
      if (graph.get(fromto(1)) == None){
        graph = graph.updated(fromto(1),Set())
      }
    }
    graph
  }

  def createGraphFromEdgePairList[T](edgeList:Seq[Seq[T]]):Map[T,Set[T]] = {
    var graph = Map[T, Set[T] ]()
    edgeList.foreach(fromto => {
      if (graph.get(fromto(0)) == None){
        if (fromto(0) != fromto(1)){
          graph = graph.updated(fromto(0),Set(fromto(1)))
        }else{
          graph = graph.updated(fromto(0),Set())
        }
      }else{
        if (fromto(0) != fromto(1)){
          graph = graph.updated(fromto(0),graph.getOrElse(fromto(0),Set() ) + fromto(1))
        }
      }
      if (graph.get(fromto(1)) == None){
        graph = graph.updated(fromto(1),Set())
      }
    })
    graph
  }

  def stringEdgeToPairEdgeWithDelemeterAttr(edgeList:Seq[String],delemeter:String):Seq[Seq[String]] = {
    def go(rest:Seq[String],acc:List[List[String]]):List[List[String]] = rest match {
      case Seq(head, tail @ _*)=> go(tail,head.split(delemeter).toList::acc)
      case Seq() => acc
      case _ => acc
    }
    go(edgeList,Nil)
  }

  def stringEdgeToPairEdgeWithArrowDelemeter(edgeList:Seq[String]):Seq[Seq[String]] = {
    val arrowDelemeter:String = "->"
    def go(rest:Seq[String],acc:List[List[String]]):List[List[String]] = rest match {
      case Seq(head, tail @ _*)=> go(tail,head.split(arrowDelemeter).toList::acc)
      case Seq() => acc
      case _ => acc
    }
    go(edgeList,Nil)
  }

  def elementaryCyclesSearch[T](adjList:Map[T,Set[T]]):Seq[Seq[T]] = {
    var G = adjList
    val graphNode:List[T] = adjList.keySet.toList
    var cycles: Seq[Seq[T]] = List()
    var blocked:Map[T,Boolean] = Map()
    var B : Map[T,List[T]] = Map()
    var closed : Map[T,Boolean] = Map()
    var path: List[T] = List()
    var stack =  List[Map[T,List[T]]]()


    def unblock(thisnode:T):Unit = {
      var stackSet = List(thisnode)
      while (stackSet.nonEmpty){
        var node = stackSet.head
        stackSet = stackSet.tail
        if (blocked.getOrElse(node,false)){
          blocked = blocked.updated(node,false)
          B.getOrElse(node,Vector()).foreach(x => {
            stackSet = x::stackSet
          })
          B = B.updated(node,List())
        }
      }
    }

    def getAns(unit: Unit):Seq[Seq[T]] = {

      var sccs:List[Set[T]] = sccfunc(G)
      while (sccs.nonEmpty){
        var scc:Set[T] = sccs.head
        sccs = sccs.tail
        var startnode = scc.head
        scc = scc.tail
        path = List(startnode)
        // init blocked and B
        graphNode.iterator.foreach(x=> {
          blocked = blocked.updated(x,x == startnode)
          B = B.updated(x,List())
        })
        stack = Map[T,List[T]](startnode  -> G.getOrElse(startnode,List[T]()).toList)::stack
        while (stack.nonEmpty){
          var thisnodeNbrs:Map[T,List[T]] = stack.head

          var thisnode:T =thisnodeNbrs.keySet.toList.head
          var nbrs = thisnodeNbrs.getOrElse(thisnode,List[T]())
          var continueFlag : Boolean = false
          if (nbrs.nonEmpty){
            stack = stack.tail
            var nextnode = nbrs.head
            nbrs = nbrs.tail
            stack = Map[T,List[T]](thisnode  -> nbrs)::stack
            if (nextnode == startnode){
              cycles = path+:cycles
              path.foreach(x => {
                closed = closed.updated(x,true)
              })
            }else if (! blocked.getOrElse(nextnode,true)){
              path = nextnode::path
              stack = Map[T,List[T]](nextnode  -> G.getOrElse(nextnode,List[T]()).toList)::stack
              closed = closed.updated(nextnode,false)
              blocked = blocked.updated(nextnode,true)
              continueFlag = true
            }
          }

          if (! continueFlag) {
            if (nbrs.isEmpty) {
              if (closed.getOrElse(thisnode, false)) {
                unblock(thisnode)
              } else {
                G.getOrElse(thisnode, Set()).iterator.foreach(nbr => {
                  var Bnbr = B.getOrElse(nbr, List())
                  if (!Bnbr.contains(thisnode)) {
                    B = B.updated(nbr, thisnode::Bnbr)
                  }
                })
              }
              stack = stack.tail
              path = path.tail
            }
          }
        }
        G = remove_node(G, startnode)
        var H = subgraph(G, scc)
        sccs = sccs++sccfunc(H)
      }

      cycles
    }
    getAns(())

  }

  def subgraph[T](G:Map[T,Set[T]], vertices:Set[T]):Map[T,Set[T]] = {
    var graph = Map[T, Set[T] ]()
    vertices.iterator.foreach(v => {
      graph = graph.updated(v, G.getOrElse(v,Set()).intersect(vertices) )
    })
    graph
  }
  def remove_node[T](G:Map[T,Set[T]],target:T):Map[T,Set[T]] = {
    var graph = Map[T, Set[T] ]()
    graph = G - target
    G.keySet.iterator.foreach(k => {
      graph = graph.updated(k,graph.getOrElse(k,Set()) - target)
    })
    graph
  }
  def sccfunc[T](graph: Map[T,Set[T]]) : List[Set[T]] = {
    // strongly connected component
    val allVertices : Set[T] = graph.keySet ++ graph.values.flatten

    var index = 0
    var indices  : Map[T,Int] = Map.empty
    var lowLinks : Map[T,Int] = Map.empty
    var components : List[Set[T]] = Nil
    var s : List[T] = Nil

    def strongConnect(v: T) {
      indices  = indices.updated(v, index)
      lowLinks = lowLinks.updated(v, index)
      index += 1
      s = v :: s

      for (w <- graph.getOrElse(v, Set.empty)) {
        if (!indices.isDefinedAt(w)) {
          strongConnect(w)
          lowLinks = lowLinks.updated(v, lowLinks(v) min lowLinks(w))
        } else if (s.contains(w)) {
          lowLinks = lowLinks.updated(v, lowLinks(v) min indices(w))
        }
      }

      if (lowLinks(v) == indices(v)) {
        var c : Set[T] = Set.empty
        var stop = false
        do {
          val x :: xs = s
          c = c + x
          s = xs
          stop = x == v
        } while (!stop)

        components = c :: components
      }
    }

    for (v <- allVertices) {
      if (!indices.isDefinedAt(v)) {
        strongConnect(v)
      }
    }

    components.reverse
  }

}//aatGraphModules End

/*
array 조작함수
array 타입의 컬럼을 조작함
 */
class arraySlice_Str extends UDF3[Seq[String],Int,Int, Seq[String]] {
  override def call(array: Seq[String],from:Int,to:Int): Seq[String] = {
    aatAlgo.arraySlice(array,from,to)
  }
}

class arraySlice_Int extends UDF3[Seq[Int],Int,Int, Seq[Int]] {
  override def call(array: Seq[Int],from:Int,to:Int): Seq[Int] = {
    aatAlgo.arraySlice(array,from,to)
  }
}
class arraySlice_Double extends UDF3[Seq[Double],Int,Int, Seq[Double]] {
  override def call(array: Seq[Double],from:Int,to:Int): Seq[Double] = {
    aatAlgo.arraySlice(array,from,to)
  }
}




/*
LCS 계산
입력으로 어떤 타입을 원소로 갖는 시퀀스가 들어오느냐에 따라 UDF 를 모두 만든다
 */
class LCS_Str extends UDF2[Seq[String],Seq[String], Int] {
  override def call(db: Seq[String],query: Seq[String]): Int = {
    aatAlgo.lcs(db,query) 
  }
}

class LCS_Int extends UDF2[Seq[Int],Seq[Int], Int] {
  override def call(db: Seq[Int],query: Seq[Int]): Int = {
    aatAlgo.lcs(db,query) 
  }
}

class LCS_Double extends UDF3[Seq[Double],Seq[Double], Int,Int] {
  override def call(db: Seq[Double],query: Seq[Double],ndigits:Int): Int = {
    aatAlgo.lcs(db.map(x=> round(x*pow(10.0,ndigits)).asInstanceOf[Int] ),
                query.map(x=> round(x*pow(10.0,ndigits)).asInstanceOf[Int] )
               )
  }
}

class LCSInRange_Str extends UDF4[Seq[String],Seq[String],Int,Int, Int] {
  override def call(db: Seq[String], query: Seq[String], from: Int, to: Int): Int = {
    aatAlgo.lcs(db.slice(from,to),query.slice(from,to))
  }
}

class LCSInRange_Int extends UDF4[Seq[Int],Seq[Int],Int,Int, Int] {
  override def call(db: Seq[Int], query: Seq[Int], from: Int, to: Int): Int = {
    aatAlgo.lcs(db.slice(from,to),query.slice(from,to))
  }
}

class LCSInRange_Double extends UDF5[Seq[Int],Seq[Int],Int,Int,Int, Int] {
  override def call(db: Seq[Int], query: Seq[Int], ndigits:Int,from: Int, to: Int): Int = {
    aatAlgo.lcs(db.slice(from,to).map(x=> round(x*pow(10.0,ndigits)).asInstanceOf[Int] ),
                query.slice(from,to).map(x=> round(x*pow(10.0,ndigits)).asInstanceOf[Int] )
               )
  }
}
// actionLCS 모델을 위한 것
class LCS_AAT extends UDF2[Seq[String],Seq[String], Seq[Int]] {
  override def call(db: Seq[String],query: Seq[String]): Seq[Int] = {
    val minsize = min(db.size,query.size)

    def go(n:Int):List[Int] = {
      if (minsize < (n+1)*1000 ){
        go(n-1)
      } else if (n<0){
        Nil
      } else if (n == 0){
        List(aatAlgo.lcs(db.slice(n*1000,(n+1)*1000),query.slice(n*1000,(n+1)*1000)))
      }else{
        aatAlgo.lcs(db.slice(n*1000,(n+1)*1000),query.slice(n*1000,(n+1)*1000))::go(n-1)
      }
    }
    go(4).reverse
  }
}


/*
DTW 계산
입력으로 어떤 타입을 원소로 갖는 시퀀스가 들어오느냐에 따라 UDF 를 모두 만든다
 */

class YWDTW_Combination_str extends UDF4[Seq[String],Int, Int, Int, Seq[Double] ] {
  override def call(sequenceVal: Seq[String],subSeqSize:Int,offSet:Int,by:Int): Seq[Double] = {
    val dist:(String,String) => Double =  (x:String,y:String) => if (x==y){0.0} else{1.0}
    val seqSize = sequenceVal.size
    @annotation.tailrec
    def go(is:Int,ie:Int,js:Int,je:Int,acc:List[Double]):List[Double] = {
      if (is < 0 ){
        acc
      }else if (js < 0){
        go(is-by,ie-by,is-by-subSeqSize,is-by,acc)
      }else{
        go(is,ie,js-by,je-by,aatAlgo.yw_dtw(sequenceVal.slice(is,ie), sequenceVal.slice(js,je))(dist)::acc)
        //min(aatAlgo.yw_dtw(sequenceVal.slice(is,ie), sequenceVal.slice(js,je))(dist),go(is,ie,js-by,je-by))
      }
    }
    //db_start,db_end, query_start, query_end
    go(seqSize-subSeqSize-offSet,seqSize,seqSize-(subSeqSize*2)-offSet,seqSize-subSeqSize-offSet,Nil)
  }
}

class YWDTW_Combination_double extends UDF4[Seq[Double],Int, Int, Int, Seq[Double] ] {
  override def call(sequenceVal: Seq[Double],subSeqSize:Int,offSet:Int,by:Int): Seq[Double] = {
    val dist:(Double,Double) => Double =  (x:Double,y:Double) => abs(x-y)
    val seqSize = sequenceVal.size
    @annotation.tailrec
    def go(is:Int,ie:Int,js:Int,je:Int,acc:List[Double]):List[Double] = {
      if (is < 0 ){
        acc
      }else if (js < 0){
        go(is-by,ie-by,is-by-subSeqSize,is-by,acc)
      }else{
        go(is,ie,js-by,je-by,aatAlgo.yw_dtw(sequenceVal.slice(is,ie), sequenceVal.slice(js,je))(dist)::acc)
        //min(aatAlgo.yw_dtw(sequenceVal.slice(is,ie), sequenceVal.slice(js,je))(dist),go(is,ie,js-by,je-by))
      }
    }
    //db_start,db_end, query_start, query_end
    go(seqSize-subSeqSize-offSet,seqSize,seqSize-(subSeqSize*2)-offSet,seqSize-subSeqSize-offSet,Nil)
  }
}


class YWDTW_Str extends UDF2[Seq[String],Seq[String], Double] {
  override def call(db: Seq[String],query: Seq[String]): Double= {
    val dist:(String,String) => Double =  (x:String,y:String) => if (x==y){0.0} else{1.0}
    aatAlgo.yw_dtw(db,query)(dist)
  }
}

class YWDTW_Double extends UDF2[Seq[Double],Seq[Double], Double] {
  override def call(db: Seq[Double],query: Seq[Double]): Double= {
    val dist:(Double,Double) => Double =  (x:Double,y:Double) => abs(x-y)
    aatAlgo.yw_dtw(db,query)(dist)
  }
}

class DTW_Str extends UDF2[Seq[String],Seq[String], Double] {
  override def call(db: Seq[String],query: Seq[String]): Double= {
    val dist:(String,String) => Double =  (x:String,y:String) => if (x==y){0.0} else{1.0}
    aatAlgo.dtw(db,query)(dist)
  }
}

class DTW_Int extends UDF2[Seq[Int],Seq[Int], Double] {
  override def call(db: Seq[Int],query: Seq[Int]): Double= {
    val dist:(Int,Int) => Double =  (x:Int,y:Int) => abs(x-y).asInstanceOf[Double]
    aatAlgo.dtw(db,query)(dist)
  }
}

class DTW_Double extends UDF2[Seq[Double],Seq[Double], Double] {
  override def call(db: Seq[Double],query: Seq[Double]): Double= {
    val dist:(Double,Double) => Double =  (x:Double,y:Double) => abs(x-y)
    aatAlgo.dtw(db,query)(dist)
  }
}

class DTWInRange_Str extends UDF4[Seq[String],Seq[String],Int,Int, Double] {
  override def call(db: Seq[String], query: Seq[String], from: Int, to: Int): Double = {
    val dist:(String,String) => Double =  (x:String,y:String) => if (x==y){0.0} else{1.0}
    aatAlgo.dtw(db.slice(from,to),query.slice(from,to))(dist)
  }
}

class DTWInRange_Int extends UDF4[Seq[Int],Seq[Int],Int,Int, Double] {
  override def call(db: Seq[Int], query: Seq[Int], from: Int, to: Int): Double = {
    val dist:(Int,Int) => Double =  (x:Int,y:Int) => abs(x-y).asInstanceOf[Double]
    aatAlgo.dtw(db.slice(from,to),query.slice(from,to))(dist)
  }
}

class DTWInRange_Double  extends UDF4[Seq[Double],Seq[Double],Int,Int, Double] {
  override def call(db: Seq[Double], query: Seq[Double], from: Int, to: Int): Double = {
    val dist:(Double,Double) => Double =  (x:Double,y:Double) => abs(x-y)
    aatAlgo.dtw(db.slice(from,to),query.slice(from,to))(dist)
  }
}

class LMV extends UDF1[Seq[Double], collection.mutable.Map[String,Double]] {
  override def call(seq: Seq[Double]): collection.mutable.Map[String,Double] = {
    aatAlgo.lmv(seq)
  }
}

/*
Graph 구조 관련 계산
시퀀스 graph 에서 feature 를 찾는데 사용함
 */

class elementaryCycleSearchWithArrowEdgeStringList extends UDF1[Seq[String], Seq[Seq[String]]] {
  override def call(edgeList: Seq[String]): Seq[Seq[String]] = {
    aatGraphModules.elementaryCyclesSearch(
      aatGraphModules.createGraphFromEdgePairList(
        aatGraphModules.stringEdgeToPairEdgeWithDelemeterAttr(edgeList,"->")
      )
    )
  }
}

