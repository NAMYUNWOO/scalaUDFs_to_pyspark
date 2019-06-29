package aatudfs
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.api.java.UDF4
import org.apache.spark.sql.api.java.UDF5

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

}

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

