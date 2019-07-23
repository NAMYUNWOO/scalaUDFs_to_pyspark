package elementaryCycles
/*
Original paper: Donald B Johnson. "Finding all the elementary circuits of a directed graph." SIAM Journal on Computing. 1975.4
정말 스칼라스럽지 않은데 곧 고칠예정임
import scala.collection.mutable
*/


object SCC {

  def createGraph(edgeList:Seq[String]):Map[String,Set[String]] = {
    var graph = Map[String, Set[String] ]()
    for ( i <- edgeList.indices){
      val fromto:Seq[String] =  edgeList(i).split("->")
      if (graph.get(fromto(0)) == None){
        graph = graph.updated(fromto(0),Set(fromto(1)))
      }else{
        graph = graph.updated(fromto(0),graph.getOrElse(fromto(0),Set() ) + fromto(1))
      }
      if (graph.get(fromto(1)) == None){
        graph = graph.updated(fromto(1),Set())
      }
    }
    graph
  }

  def getAdjacencyList[T](s:T,sccs:List[Set[T]]):Boolean = {
    for (seti <- sccs.iterator){
      if (seti(s))
        return true
    }
    false
  }

  def elementaryCyclesSearch[T](adjList:Map[T,Set[T]]):Seq[Seq[T]] = {
    var G = adjList
    val graphNode:List[T] = adjList.keySet.toList
    var cycles: Seq[Seq[T]] = List()
    var blocked:Map[T,Boolean] = Map()
    var B : Map[T,Vector[T]] = Map()
    var closed : Map[T,Boolean] = Map()
    var path: List[T] = List()
    var stack =  List[Map[T,Set[T]]]()


    def unblock(thisnode:T):Unit = {
      var stackSet = List(thisnode)
      while (stackSet.nonEmpty){
        var node = stackSet.last
        stackSet = stackSet.dropRight(1)
        if (blocked.getOrElse(node,false)){
          blocked = blocked.updated(node,false)
          B.getOrElse(node,Vector()).foreach(x => {
            stackSet = stackSet :+ x
          })
          B = B.updated(node,Vector())
        }
      }
    }

    def getAns(unit: Unit):Seq[Seq[T]] = {

      var sccs:List[Set[T]] = sccfunc(G)
      while (sccs.nonEmpty){
        var scc:Set[T] = sccs.last
        sccs = sccs.dropRight(1)
        var startnode = scc.last
        scc = scc.dropRight(1)
        path = List(startnode)
        // init blocked and B
        graphNode.iterator.foreach(x=> {
          blocked = blocked.updated(x,x == startnode)
          B = B.updated(x,Vector())
        })
        stack = stack :+ Map[T,Set[T]](startnode  -> G.getOrElse(startnode,Set[T]()))
        while (stack.nonEmpty){
          var thisnodeNbrs:Map[T,Set[T]] = stack.last

          var thisnode:T =thisnodeNbrs.keySet.toList.head
          var nbrs = thisnodeNbrs.getOrElse(thisnode,Set[T]()).toList
          var continueFlag : Boolean = false
          if (nbrs.nonEmpty){
            stack = stack.dropRight(1)
            var nextnode = nbrs.last
            nbrs = nbrs.dropRight(1)
            stack = stack :+ Map[T,Set[T]](thisnode  -> nbrs.toSet)
            if (nextnode == startnode){
              cycles = cycles :+ path
              path.foreach(x => {
                closed = closed.updated(x,true)
              })
            }else if (! blocked.getOrElse(nextnode,true)){
              path = path :+ nextnode
              stack = stack :+ Map[T,Set[T]](nextnode  -> G.getOrElse(nextnode,Set[T]()))
              closed = closed - nextnode//closed.updated(nextnode,false)
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
                  var Bnbr = B.getOrElse(nbr, Vector())
                  if (!Bnbr.contains(thisnode)) {
                    B = B.updated(nbr, Bnbr :+ thisnode)
                  }
                })
              }
              stack = stack.dropRight(1)
              path = path.dropRight(1)
            }
          }
        }
        G = remove_node(G, startnode)
        var H = subgraph(G, scc)
        sccs = sccs++sccfunc(H)
      }

      cycles
    }
    return getAns(())

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
    // The first part is a shameless adaptation from Wikipedia
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


}
/*

val tempList = List("[Char_ChannelMove]->[Char_ChannelMove]", "[Char_ChannelMove]->[Char_FieldMove-220000000-]", "[Char_ChannelMove]->[Char_FieldMove-220010600-]", "[Char_ChannelMove]->[Char_FieldMove-220010700-]", "[Char_ChannelMove]->[Char_GameMoneyGet-2-]", "[Char_ChannelMove]->[Char_ItemDiscard]", "[Char_Dead-300030000-]->[Char_FieldMove-300000000-]", "[Char_FieldMove-220000000-, Char_GameMoneyUse-0-2-]->[Char_AccessDisconnect]", "[Char_FieldMove-220000000-, Char_GameMoneyUse-0-2-]->[Char_FieldMove-220010500-]", "[Char_FieldMove-220000000-]->[Char_FieldMove-300000100-, Char_GameMoneyUse-0-2-]", "[Char_FieldMove-220000000-]->[Char_FieldMove-300000100-]", "[Char_FieldMove-220010500-]->[Char_FieldMove-220010600-]", "[Char_FieldMove-220010600-]->[Char_ChannelMove]", "[Char_FieldMove-220010600-]->[Char_FieldMove-220010700-]", "[Char_FieldMove-220010700-]->[Char_ChannelMove]", "[Char_FieldMove-220010700-]->[Char_FieldMove-220010600-]", "[Char_FieldMove-300000000-]->[Char_Dead-300030000-]", "[Char_FieldMove-300000000-]->[Char_FieldMove-300000100-]", "[Char_FieldMove-300000000-]->[Char_FieldMove-300030000-]", "[Char_FieldMove-300000000-]->[Char_GameMoneyGet-1-]", "[Char_FieldMove-300000000-]->[Char_ItemDiscard]", "[Char_FieldMove-300000100-, Char_GameMoneyUse-0-2-]->[Char_ItemDiscard]", "[Char_FieldMove-300000100-]->[Char_FieldMove-220000000-, Char_GameMoneyUse-0-2-]", "[Char_FieldMove-300000100-]->[Char_FieldMove-300000000-]", "[Char_FieldMove-300030000-]->[Char_Dead-300030000-]", "[Char_GameMoneyGet-1-]->[Char_GameMoneyGet-1-]", "[Char_GameMoneyGet-1-]->[Char_GameMoneyGet-16-]", "[Char_GameMoneyGet-16-]->[Char_FieldMove-300000100-]", "[Char_GameMoneyGet-16-]->[Char_GameMoneyGet-16-]", "[Char_GameMoneyGet-2-]->[Char_ChannelMove]", "[Char_GameMoneyGet-2-]->[Char_GameMoneyGet-2-]", "[Char_ItemDiscard]->[Char_ChannelMove]", "[Char_ItemDiscard]->[Char_FieldMove-300000000-]", "[Char_ItemDiscard]->[Char_FieldMove-300030000-]", "[Char_ItemDiscard]->[Char_ItemDiscard]")
var res = SCC.elementaryCyclesSearch(SCC.createGraph(tempList))
res.size
*/