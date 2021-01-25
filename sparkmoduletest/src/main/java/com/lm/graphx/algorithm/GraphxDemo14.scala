package com.lm.graphx.algorithm

import com.lm.spark.model.{GroupMem, MemRel}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @Classname GraphxDemo14
  * @Description TODO
  * @Date 2020/8/3 14:07
  * @Created by limeng
  */


object GraphxDemo14 {

    def main(args: Array[String]): Unit = {
      val session = SparkSession.builder().appName("GraphxDemo14").master("local[*]").getOrCreate()



      val sc = session.sparkContext

      val root: RDD[(Long, Long)] = sc.makeRDD(Array((2L, 2L), (8L, 8L), (7L, 7L)))

      val v = sc.makeRDD(Array((1L,0),(2L,0),(3L,0),(4L,0),(5L,0),(6L,0),(7L,0),(8L,0)))


      val value: RDD[Edge[Double]] = sc.makeRDD(Array(Edge(2L, 1L, 40D), Edge(3L, 1L, 20D), Edge(4L, 1L, 60D), Edge(5L, 1L, 20D),
        Edge(4L, 3L, 60D), Edge(2L, 3L, 40D), Edge(2L, 4L, 20D), Edge(6L, 4L, 40D),Edge(5L, 4L, 40D),Edge(2L, 6L, 40D),Edge(5L, 6L, 40D),Edge(7L, 6L, 20D),Edge(8L, 5L, 40D)))
      val e =value

      val graph: Graph[Int, Double] = Graph(v, e)

      val value3 = graph.outerJoinVertices(root)((vid, vd, ud) => {
        val u = ud.getOrElse(Long.MaxValue)
        if (u != Long.MaxValue) {
          GroupVD(Set(MsgScore(u, u, u, 100D)), Set[MsgScore](), Set(vid))
        } else {
          GroupVD(Set[MsgScore](), Set[MsgScore](), Set())
        }
      })
      println("value3")
      value3.vertices.collect().foreach(println(_))


      val quaGroup = graph.outerJoinVertices(root)((vid,vd,ud)=>{
        val u = ud.getOrElse(Long.MaxValue)
        if(u!=Long.MaxValue){
          GroupVD(Set(MsgScore(u, u, u, 100D)), Set[MsgScore](), Set(vid))
        }else{
          GroupVD(Set[MsgScore](), Set[MsgScore](), Set())
        }
      }).pregel(GroupVD(Set[MsgScore](), Set[MsgScore](), Set[Long]()),
        10)(gProg, gSendMsg, gMergeMsg)

      println("quaGroup.vertices")
      quaGroup.vertices.foreach(println(_))

      val mm:RDD[GroupMem] = quaGroup.vertices.flatMap(f=>{
         f._2.accept.groupBy(_.groupId).map(m=>{
           val rel = m._2.map(msg => {
             MemRel(msg.from, msg.to, msg.score, 1)
           })
           GroupMem(f._1, m._1, m._2.map(_.score).sum, 1, 0, rel)
         })
      })

      /**
        * GroupMem(1,2,10.0,1,0,Set(2#1))
        * GroupMem(1,7,45.0,1,0,Set(5#1, 4#1))
        * GroupMem(1,3,45.0,1,0,Set(3#1))
        * GroupMem(1,6,45.0,1,0,Set(5#1, 4#1))
        * GroupMem(2,2,100.0,1,0,Set(2#2))
        * GroupMem(3,3,100.0,1,0,Set(3#3))
        * GroupMem(4,7,40.0,1,0,Set(7#4))
        * GroupMem(4,6,60.0,1,0,Set(6#4))
        * GroupMem(5,7,40.0,1,0,Set(7#5))
        * GroupMem(5,6,60.0,1,0,Set(6#5))
        * GroupMem(6,6,100.0,1,0,Set(6#6))
        * GroupMem(7,7,100.0,1,0,Set(7#7))
        */
      println("mm:")
      mm.sortBy(_.targetId).collect().foreach(println(_))

      val mb: RDD[MemRelBreadth] = mm.groupBy(_.groupId).flatMap(f => {
        f._2.flatMap(f2 => {
          f2.rel.filter(l => l.from != l.to).map(m => {
            MemRelBreadth(m.from, m.to, m.score, f._1,Double.PositiveInfinity,Double.PositiveInfinity)
          })
        })
      })

      /**
        * 2#1#2#9223372036854775807
        * 3#1#3#9223372036854775807
        * 5#1#6#9223372036854775807
        * 4#1#6#9223372036854775807
        * 6#4#6#9223372036854775807
        * 6#5#6#9223372036854775807
        * 5#1#7#9223372036854775807
        * 4#1#7#9223372036854775807
        * 7#4#7#9223372036854775807
        * 7#5#7#9223372036854775807
        */
      println("mb")
      mb.collect().foreach(println(_))

      val tail:Long = 1L


//      val v3: RDD[MVD] = mb.map(m => (m.from + "-" + m.to, MVD(m.from,m.to,m.score,Set(m.groupId)))).groupByKey().map(f => {
//        val g = f._2.map(_.groupId).toSet
//        val v = f._2.map(m => (m.from, m.to, m.score)).head
//        MVD(v._1, v._2, v._3, g)
//      })

     val v3 = mb.map(m => (m.from + "#" + m.to, MVD(m.from,m.to,m.score,Set(m.groupId)))).reduceByKey((a,b)=>{
        MVD(a.from,a.to,a.score,a.ids++b.ids)
      }).map(_._2)






      println("v3")
      v3.foreach(println(_))
      println("v3")

      val value1 = sc.parallelize(v3.filter(f=>f.to == tail).take(1).map(m=>(m.to,MemIds2(m.to,m.ids,null))))


      val g2: Graph[Double, MemRelBreadth] = Graph.fromEdges(mb.map(m => Edge(m.from, m.to, m)), Double.PositiveInfinity)

      val value2: Graph[VertexId, Double] = Graph.fromEdges(v3.map(m => Edge(m.from, m.to, m.score)), 1L)

      val roots2 = value2.aggregateMessages[(Int,Int,VertexId)](ctx=>{
        ctx.sendToSrc((1,0,ctx.srcId))
        ctx.sendToDst((0,1,ctx.srcId))
      },mergeMs).filter(_._2._1 == 0)

      println("终点：")
      roots2.map(_._1.toLong).collect().foreach(println(_))


      var result:RDD[GroupMem2] =  null
      val map = new mutable.HashMap[String,Set[GroupMem2]]

      val edge = v3.map(m=>Edge(m.from,m.to,m))
      val graph2 = Graph.fromEdges(edge,1L)

      val bredge = sc.broadcast(edge.collect())

      val keys = mm.map(m=> (m.targetId,m.groupId)).groupByKey()
      val roots3 = roots2.map(m=> (m._1.toLong,m._1.toLong)).join(keys).map(m=> (m._1,m._2._2.toSet))
      val brroots = sc.broadcast(roots3.collect())
//      roots2.map(_._1.toLong).collect().foreach(o=>{
//            var ids = sc.parallelize(v3.filter(f=>f.to == o).take(1).map(m=>(m.to,MemIds2(m.to,m.ids,null))))
//            var rids:List[(Int,MemIds2)] = List()
//            val rddb = sc.broadcast(v3.collect())
//           // diguiM(ids,v3,map,sc,rddb)
//            var index:Int = 0
//            while (!ids.isEmpty()){
//              ids = v3.map(m=>(m.to,m)).join(ids).map(m=>{
//                val path = m._2._2.path
//                if(path == null){
//                  (m._2._1.from,MemIds2(m._2._1.from,m._2._1.ids,m._1+"#"+m._2._1.from))
//                }else{
//                  (m._2._1.from,MemIds2(m._2._1.from,m._2._1.ids,path+"#"+m._2._1.from))
//                }
//
//              })
//
//              rids.++=(ids.toLocalIterator.map(m=> (index,m._2)).toList)
//              index+=1
//            }
//
//            val rids2: RDD[(Int, MemIds2)] = sc.makeRDD(rids.reverse)
//            while (index>=0){
//              diguiM2(rids2.filter(_._1 == index),map,rddb)
//              index-=1
//            }
//
//            if(result == null){
//              result = sc.makeRDD(map.values.flatten.toSeq)
//            }else{
//              result = result.union(sc.makeRDD(map.values.flatten.toSeq))
//            }
//            map.clear()
//      })


      val rddb = sc.broadcast(v3.collect())
      var result2:Set[GroupMem2] = Set[GroupMem2]()

      val r2 =roots2.map(_._1.toLong).cache()
      var rids:Array[(Int, MemIds2)] = null

      var index:Int = 0
      var tmp:Array[(Long, MemIds2)] = null
      r2.mapPartitions(p => {

        p.flatMap(o => {

          var ids: Array[(VertexId, MemIds2)] = rddb.value.filter(f => f.to == o).take(1).map(m => (m.to, MemIds2(m.to, m.ids, null)))
          val map = new mutable.HashMap[String, Set[GroupMem2]]
          val head = brroots.value.filter(f => f._1 == o).head

          val mvds = graph2.subgraph(e=> e.attr.ids.intersect(head._2) != null).edges.map(m=>m.attr).collect()
//          val mvds = rddb.value.filter(f => {
//            val longs = f.ids.intersect(head._2)
//            if (longs.isEmpty) {
//              false
//            } else {
//              true
//            }
//          })
          index = 0
          while (ids != null && !ids.isEmpty) {
            tmp = null
            mvds.map(m => (m.to, m)).foreach(f => {
              ids.foreach(f2 => {
                if (f2._1 == f._1) {
                  val path = f2._2.path
                  var tuple: (VertexId, MemIds2) = null
                  if (path == null) {
                    tuple = (f._2.from, MemIds2(f._2.from, f._2.ids, f._1 + "#" + f._2.from))
                  } else {
                    tuple = (f._2.from, MemIds2(f._2.from, f._2.ids, path + "#" + f._2.from))
                  }

                  if (tmp != null) {
                    tmp.++=(Array(tuple))
                  } else {
                    tmp = Array(tuple)
                  }

                }
              })
            })

            ids = tmp

            if (ids != null && rids == null) {
              rids = ids.map(m => {
                val path = m._2.path
                (path.split("#").length - 1, m._2)
              })
            } else if (ids != null) {
              rids = rids.union(ids.map(m => {
                val path = m._2.path
                (path.split("#").length - 1, m._2)
              }))
            }
            index += 1

          }

          while (index >= 0) {
            diguiM3(rids.filter(_._1 == index), map, rddb)
            index -= 1
          }

          map.values.flatten.toSet
        })

      })


     // cccc.collect().foreach(println(_))
//      val list = result.collect().toList
//      println(list)

//        r2.collect().foreach(o=>{
//              var ids = v3.filter(f=>f.to == o).map(m=>(m.to,MemIds2(m.to,null,null))).distinct()
//              var rids:RDD[(Int, MemIds2)] = null
//
//              var index:Int = 0
//              while (!ids.isEmpty()){
//                ids = v3.map(m=>(m.to,m)).join(ids).map(m=>{
//                  val path = m._2._2.path
//                  if(path == null){
//                    (m._2._1.from,MemIds2(m._2._1.from,m._2._1.ids,m._1+"#"+m._2._1.from))
//                  }else{
//                    (m._2._1.from,MemIds2(m._2._1.from,m._2._1.ids,path+"#"+m._2._1.from))
//                  }
//
//                })
//
//                if(rids == null){
//                  rids = ids.map(m=>{
//                    val path = m._2.path
//                    (path.split("#").length-1,m._2)
//                  })
//                } else{
//                  rids = rids.union(ids.map(m=>{
//                    val path = m._2.path
//                    (path.split("#").length-1,m._2)
//                  }))
//                }
//                index+=1
//              }
//
//
//          val tuples = rids.collect()
//          println(tuples)
//          while (index>=0){
//              diguiM2(rids.filter(_._1 == index),map,rddb)
//              index-=1
//          }
//
//          if(result == null){
//            result = sc.makeRDD(map.values.flatten.toSeq)
//          }else{
//            result = result.union(sc.makeRDD(map.values.flatten.toSeq))
//          }
//          map.clear()
//        })


      //      val gids = mb.groupBy(_.groupId).map(f=> f._1).distinct()
//      println("level")
//      val level = getLevel2(gids,g2,sc)
//      level.foreach(f=>{
//        println("--------")
//        f.collect().foreach(println(_))
//        println("--------")
//      })
//      val map = new mutable.HashMap[String,Long]
//      level.foreach(f=>{
//        println("--------")
//        f.collect().foreach(println(_))
//        println("--------")
//      })

//      for(i <- 1 to 10){
//        val v: RDD[MemRelBreadth] = level.flatMap(m=>m.filter(f=>f.tolevel.intValue() == i).toLocalIterator)
//        v.collect().foreach(f=>{
//          map.put(f.groupId.toString,f.to)
//        })
//      }


//
//
//
//      map.foreach(f=> {
//        println("toid "+f._1)
//        f._2.foreach(println(_))
//        println("toid end ")
//      })





     // rddb.unpersist()
      session.stop()
    }

  var queue = new mutable.LinkedList[Long]()

  def mergeMs(a:(Int,Int,VertexId),b:(Int,Int,VertexId)):(Int,Int,VertexId) = (a._1 + b._1,a._2+b._2,a._3)

  def diguiM3(ids:Array[(Int,MemIds2)], gg:mutable.HashMap[String,Set[GroupMem2]],rddb: Broadcast[Array[MVD]]): Unit ={
    if(ids == null || ids.isEmpty) return

    val tids =ids.map(m=> (m._2.from,m._2))

    if(gg.isEmpty){
      val v1: Array[GroupMem2] = tids.filter(f => f._2.gids.contains(f._2.from)).map(m => {
        GroupMem2(m._2.from, m._2.from, Set(MemRel2(m._2.from, m._2.from, 100D)))
      }).distinct

      val d: Array[(String, (String, String, Long, Double))] = tids.flatMap(m => {
        val ps = m._2.path.split("#")
        val s = ps.size
        val from = ps(s - 1)
        val to = ps(s - 2)
        val r = rddb.value
        val v = r.filter(f => f.from == from.toLong && f.to == to.toLong)
        m._2.gids.map(p => {
          (to, (from, to, p, v.head.score))
        })
      }).distinct


      val v2: Array[GroupMem2] = d.map(_._2).groupBy(_._2).flatMap(f => {
        val tuple = f._2.maxBy(_._4)
        f._2.filter(l=> l._4 == tuple._4).map(m=>{
          GroupMem2(m._2.toLong, m._3, Set(MemRel2(m._1.toLong, m._2.toLong, m._4)))
        })
      }).toArray


      val v3: Array[GroupMem2] = v1.union(v2)

      v3.groupBy(_.targetId).foreach(m=>{
        gg.put(m._1.toString,m._2.toSet)
      })
    }else{
      val tmp = new mutable.HashMap[String,Set[GroupMem2]]

      val v1: Array[GroupMem2] = tids.filter(f => f._2.gids.contains(f._2.from)).map(m => {
        GroupMem2(m._2.from, m._2.from, Set(MemRel2(m._2.from, m._2.from, 100D)))
      }).distinct

      v1.groupBy(_.targetId).foreach(m=>{
        gg.put(m._1.toString,m._2.toSet)
      })


      val d = tids.flatMap(m => {
        val ps = m._2.path.split("#")
        val s = ps.size
        val from = ps(s - 1)
        val to = ps(s - 2)
        val r = rddb.value
        val v = r.filter(f => f.from == from.toLong && f.to == to.toLong)
        m._2.gids.map(p => {
          (from, to, p, v.head.score)
        })
      }).distinct

      val list = d.filter(f=> ! gg.keySet.contains(f._2)).groupBy(_._2)

      //已经不存在to key
      list.foreach(f=>{
        val to = f._1

        //gid
        val gvds = f._2.groupBy(_._3).map(f2 => {
          val gid = f2._1

          val gm = f2._2.filter(l => gg.keySet.contains(l._1)).map(m=>{
            val from = m._1.toLong
            val gids = gg.getOrElse(from.toString,null).map(m2=> m2.groupId)
            (from->gids)
          }).toMap


          var tuples: Iterable[(String, String, Long, Double, Double)] = f2._2.filter(l => gg.keySet.contains(l._1)).map(f3 => {
            val size = gg.getOrElse(f3._1, null).size
            val td = f3._4 / size
            (f3._1, f3._2, f3._3, f3._4, td)
          })

          val gfrom = tuples.filter(l=> {
            val gv = gm.getOrElse(l._1.toLong,null)
            if(gv!= null && gv.contains(l._3)){
              true
            }else{
              false
            }
          }).map(_._1).toList

          tuples = tuples.filter(l=> gfrom.contains(l._1))

          val sumd= tuples.map(_._5).sum
          val rels = tuples.map(m=>MemRel2(m._1.toLong,m._2.toLong,m._4))
          GVD(to.toLong,gid.toLong,sumd,rels.toSet)
        }).filter(l=> l.rel.nonEmpty)

        val maxscore = gvds.maxBy(_.score)

        gvds.filter(l2=> maxscore.score == l2.score).toList.foreach(f4=>{
          val to = f4.from
          val v= tmp.getOrElse(to.toString,null)
          if(v == null){
            tmp.put(to.toString,Set(GroupMem2(to,f4.groupId,f4.rel)))
          }else{
            tmp.put(to.toString,v++Set(GroupMem2(to,f4.groupId,f4.rel)))
          }
        })
      })



      gg.++=(tmp)

    }

  }


  def diguiM2(ids:RDD[(Int,MemIds2)], gg:mutable.HashMap[String,Set[GroupMem2]],rddb: Broadcast[Array[MVD]]): Unit ={
    if(ids.isEmpty()) return
    val tids =ids.map(m=> (m._2.from,m._2))
    if(gg.isEmpty){
      val v1: RDD[GroupMem2] = tids.filter(f => f._2.gids.contains(f._2.from)).map(m => {
        GroupMem2(m._2.from, m._2.from, Set(MemRel2(m._2.from, m._2.from, 100D)))
      }).distinct()


      val d = tids.flatMap(m => {
        val ps = m._2.path.split("#")
        val s = ps.size
        val from = ps(s - 1)
        val to = ps(s - 2)
        val r = rddb.value
        val v = r.filter(f => f.from == from.toLong && f.to == to.toLong)
        m._2.gids.map(p => {
          (to, (from, to, p, v.head.score))
        })
      }).distinct()

      val v2: RDD[GroupMem2] = d.map(_._2).groupBy(_._2).flatMap(f => {
        val tuple = f._2.maxBy(_._4)
        f._2.filter(l=> l._4 == tuple._4).map(m=>{
          GroupMem2(m._2.toLong, m._3, Set(MemRel2(m._1.toLong, m._2.toLong, m._4)))
        })
      })


      val v3 = v1.union(v2)

      v3.groupBy(_.targetId).collect().foreach(m=>{
        gg.put(m._1.toString,m._2.toSet)
      })

    }else{
      //  val bmap = sc.broadcast(gg)
      val tmp = new mutable.HashMap[String,Set[GroupMem2]]

      val v1: RDD[GroupMem2] = tids.filter(f => f._2.gids.contains(f._2.from)).map(m => {
        GroupMem2(m._2.from, m._2.from, Set(MemRel2(m._2.from, m._2.from, 100D)))
      }).distinct()

      v1.groupBy(_.targetId).collect().foreach(m=>{
        gg.put(m._1.toString,m._2.toSet)
      })



      val d = tids.flatMap(m => {
        val ps = m._2.path.split("#")
        val s = ps.size
        val from = ps(s - 1)
        val to = ps(s - 2)
        val r = rddb.value
        val v = r.filter(f => f.from == from.toLong && f.to == to.toLong)
        m._2.gids.map(p => {
          (from, to, p, v.head.score)
        })
      }).distinct()

      val list = d.filter(f=> ! gg.keySet.contains(f._2)).groupBy(_._2).collect()

      //已经不存在to key
      list.foreach(f=>{
        val to = f._1

        //gid
        val gvds = f._2.groupBy(_._3).map(f2 => {
          val gid = f2._1

          val gm = f2._2.filter(l => gg.keySet.contains(l._1)).map(m=>{
            val from = m._1.toLong
            val gids = gg.getOrElse(from.toString,null).map(m2=> m2.groupId)
            (from->gids)
          }).toMap


          var tuples: Iterable[(String, String, Long, Double, Double)] = f2._2.filter(l => gg.keySet.contains(l._1)).map(f3 => {
            val size = gg.getOrElse(f3._1, null).size
            val td = f3._4 / size
            (f3._1, f3._2, f3._3, f3._4, td)
          })

          val gfrom = tuples.filter(l=> {
            val gv = gm.getOrElse(l._1.toLong,null)
            if(gv!= null && gv.contains(l._3)){
              true
            }else{
              false
            }
          }).map(_._1).toList

          tuples = tuples.filter(l=> gfrom.contains(l._1))

          val sumd= tuples.map(_._5).sum
          val rels = tuples.map(m=>MemRel2(m._1.toLong,m._2.toLong,m._4))
          GVD(to.toLong,gid.toLong,sumd,rels.toSet)
        }).filter(l=> l.rel.nonEmpty)

        val maxscore = gvds.maxBy(_.score)

        gvds.filter(l2=> maxscore.score == l2.score).toList.foreach(f4=>{
          val to = f4.from
          val v= tmp.getOrElse(to.toString,null)
          if(v == null){
            tmp.put(to.toString,Set(GroupMem2(to,f4.groupId,f4.rel)))
          }else{
            tmp.put(to.toString,v++Set(GroupMem2(to,f4.groupId,f4.rel)))
          }
        })
      })



      gg.++=(tmp)

    }

  }


  def diguiM(ids:RDD[(Long,MemIds2)], rdd:RDD[MVD], gg:mutable.HashMap[String,Set[GroupMem2]], sc:SparkContext,rddb: Broadcast[Array[MVD]]): Unit ={
    if(ids.isEmpty){

        return
    }

    val tids = rdd.map(m=>(m.to,m)).join(ids).map(m=>{
      val path = m._2._2.path
      if(path == null){
        (m._2._1.from,MemIds2(m._2._1.from,m._2._1.ids,m._1+"#"+m._2._1.from))
      }else{
        (m._2._1.from,MemIds2(m._2._1.from,m._2._1.ids,path+"#"+m._2._1.from))
      }

    })


    diguiM(tids,rdd,gg,sc,rddb)



    if(gg.isEmpty){
      val v1: RDD[GroupMem2] = tids.filter(f => f._2.gids.contains(f._2.from)).map(m => {
        GroupMem2(m._1, m._1, Set(MemRel2(m._1, m._1, 100D)))
      }).distinct()


      val d = tids.flatMap(m => {
        val ps = m._2.path.split("#")
        val s = ps.size
        val from = ps(s - 1)
        val to = ps(s - 2)
        val r = rddb.value
        val v = r.filter(f => f.from == from.toLong && f.to == to.toLong)
        m._2.gids.map(p => {
          (to, (from, to, p, v.head.score))
        })
      }).distinct()

      val v2: RDD[GroupMem2] = d.map(_._2).groupBy(_._2).flatMap(f => {
        val tuple = f._2.maxBy(_._4)
        f._2.filter(l=> l._4 == tuple._4).map(m=>{
          GroupMem2(m._2.toLong, m._3, Set(MemRel2(m._1.toLong, m._2.toLong, m._4)))
        })
      })


      val v3 = v1.union(v2)

      v3.groupBy(_.targetId).collect().foreach(m=>{
        gg.put(m._1.toString,m._2.toSet)
      })

    }else{
    //  val bmap = sc.broadcast(gg)
      val tmp = new mutable.HashMap[String,Set[GroupMem2]]

      val v1: RDD[GroupMem2] = tids.filter(f => f._2.gids.contains(f._2.from)).map(m => {
        GroupMem2(m._1, m._1, Set(MemRel2(m._1, m._1, 100D)))
      }).distinct()

      v1.groupBy(_.targetId).collect().foreach(m=>{
        gg.put(m._1.toString,m._2.toSet)
      })



      val d = tids.flatMap(m => {
        val ps = m._2.path.split("#")
        val s = ps.size
        val from = ps(s - 1)
        val to = ps(s - 2)
        val r = rddb.value
        val v = r.filter(f => f.from == from.toLong && f.to == to.toLong)
        m._2.gids.map(p => {
         (from, to, p, v.head.score)
        })
      }).distinct()

      val list = d.filter(f=> ! gg.keySet.contains(f._2)).groupBy(_._2).collect()

      //已经不存在to key
      list.foreach(f=>{
        val to = f._1

        //gid
        val gvds = f._2.groupBy(_._3).map(f2 => {
          val gid = f2._1

          val gm = f2._2.filter(l => gg.keySet.contains(l._1)).map(m=>{
            val from = m._1.toLong
            val gids = gg.getOrElse(from.toString,null).map(m2=> m2.groupId)
            (from->gids)
          }).toMap


          var tuples: Iterable[(String, String, Long, Double, Double)] = f2._2.filter(l => gg.keySet.contains(l._1)).map(f3 => {
            val size = gg.getOrElse(f3._1, null).size
            val td = f3._4 / size
            (f3._1, f3._2, f3._3, f3._4, td)
          })

         val gfrom = tuples.filter(l=> {
            val gv = gm.getOrElse(l._1.toLong,null)
            if(gv!= null && gv.contains(l._3)){
              true
            }else{
              false
            }
          }).map(_._1).toList

          tuples = tuples.filter(l=> gfrom.contains(l._1))

          val sumd= tuples.map(_._5).sum
          val rels = tuples.map(m=>MemRel2(m._1.toLong,m._2.toLong,m._4))
          GVD(to.toLong,gid.toLong,sumd,rels.toSet)
        }).filter(l=> l.rel.nonEmpty)

        val maxscore = gvds.maxBy(_.score)

        gvds.filter(l2=> maxscore.score == l2.score).toList.foreach(f4=>{
          val to = f4.from
          val v= tmp.getOrElse(to.toString,null)
          if(v == null){
            tmp.put(to.toString,Set(GroupMem2(to,f4.groupId,f4.rel)))
          }else{
            tmp.put(to.toString,v++Set(GroupMem2(to,f4.groupId,f4.rel)))
          }
        })
      })



      gg.++=(tmp)


    }



  }

  def getLevel2(gids:RDD[Long],g2:Graph[Double, MemRelBreadth],sc:SparkContext) ={
    val bg = sc.broadcast(g2)

    gids.map(f=>{
      val gid = f
      val tg = bg.value.subgraph(_.attr.groupId == gid)


      val initialGraph  = tg.mapVertices((id,_)=> if(id == gid) 0.0 else Double.PositiveInfinity)

      val vprog = {(id:VertexId,attr:Double,msg:Double) => math.min(attr,msg)}

      val sendMessage = {(triplet: EdgeTriplet[Double, MemRelBreadth]) =>
        var iter:Iterator[(VertexId, Double)] = Iterator.empty

        val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
        val isDstMarked = triplet.dstAttr != Double.PositiveInfinity

        if(!(isSrcMarked && isDstMarked)){
          if(isSrcMarked){
            iter = Iterator((triplet.dstId,triplet.srcAttr + 1))
          }else{
            iter = Iterator((triplet.srcId,triplet.dstAttr + 1))
          }
        }
        iter
      }
      val reduceMessage = {(a:Double,b:Double) => math.min(a,b)}

      val bfs = initialGraph.pregel(Double.PositiveInfinity,10)(vprog,sendMessage,reduceMessage)


      bfs.vertices.sortBy(a=>a._2).filter(_._2 != Double.PositiveInfinity)

//
//      bfs.edges.map(m=> (m.attr.from,m.attr)).join(b).map(m=>{
//        (m._2._1.to,MemRelBreadth(m._2._1.from,m._2._1.to,m._2._1.score,m._2._1.groupId,m._2._2,Double.PositiveInfinity))
//      }).join(b).map(m=>{
//        MemRelBreadth(m._2._1.from,m._2._1.to,m._2._1.score,m._2._1.groupId,m._2._1.fromlevel,m._2._2)
//      })

    })

  }

  def getMemRelBreadth(rdd:RDD[MemRelBreadth],ids:RDD[(Long,Long)]): Unit ={
//      if(ids.isEmpty() ){
//        return  ids.map(_._1).collect().mkString("#")
//      }else{
//       val tids = rdd.map(m=> (m.from,m)).join(ids).map(m=>{
//          (m._2._1.to,m._2._1.to)
//        })
//        val str = getMemRelBreadth(rdd,tids)
//      }





  }


  def gProgPath(vertexId: Long,vd:MemRelBreadth2,msg:MemRelBreadth2): MemRelBreadth2 ={
    val vgid = vd.groupId


    if(vgid != Long.MaxValue){
      MemRelBreadth2(vd.groupId,vd.path,vd.ids++msg.ids)
    }else {
      MemRelBreadth2(msg.groupId,msg.path,vd.ids++msg.ids)
    }
  }

  def gMergePath(vd:MemRelBreadth2,msg:MemRelBreadth2): MemRelBreadth2 ={
    val vgid = vd.groupId
    if(vgid != Long.MaxValue){
      MemRelBreadth2(vd.groupId,vd.path,vd.ids++msg.ids)
    }else{
      MemRelBreadth2(msg.groupId,msg.path,vd.ids++msg.ids)
    }
  }

  def gSendPath(triplet: EdgeTriplet[MemRelBreadth2, Double]): Iterator[(Long, MemRelBreadth2)] ={
    val from = triplet.srcId
    val to = triplet.dstId


    val gid = triplet.srcAttr.groupId

    val spath = triplet.srcAttr.path
    val epath = triplet.dstAttr.path
    var srpath:String =""
    var erpath:String =""
    if(spath != null){
      srpath = spath+"#"+to
    }else{
      srpath = to.toString
    }

    if(epath != null){
      erpath = epath+"#"+to
    }else{
      erpath = to.toString
    }

    Iterator((triplet.dstId, MemRelBreadth2(gid, srpath, triplet.srcAttr.ids ++ Set(triplet.dstId))),
      (triplet.srcId, MemRelBreadth2(triplet.dstAttr.groupId, erpath, triplet.srcAttr.ids)))



  }


  def gProg(vertexId: Long,vd: GroupVD,msg: GroupVD): GroupVD ={
     val accept = vd.accept ++ msg.accept.groupBy(a=>a.groupId).flatMap(f=> f._2)

      GroupVD(accept,vd.sent ++ msg.sent,msg.ids++vd.ids)
  }

  def gSendMsg(triplet: EdgeTriplet[GroupVD, Double]): Iterator[(Long, GroupVD)] = {
    val from = triplet.srcId
    val to = triplet.dstId
    val unsent = triplet.srcAttr.accept.diff(triplet.srcAttr.sent)

    if(unsent.nonEmpty){
      //val contrlKey = triplet.srcAttr.accept.groupBy(u=>u.groupId).map(m=>{m._1 -> m._2.map(_.score).sum})
      if(!triplet.srcAttr.ids.contains(triplet.dstId)){

        val msg = unsent.map(a => {
          MsgScore(a.groupId, from, to, triplet.attr)
        })

        Iterator((triplet.dstId, GroupVD(msg, Set[MsgScore](), triplet.srcAttr.ids ++ Set(triplet.dstId))),
          (triplet.srcId, GroupVD(Set[MsgScore](), msg, triplet.srcAttr.ids)))
      }else{
        Iterator.empty
      }
    }else{
      Iterator.empty
    }
  }

  def gMergeMsg(ma: GroupVD, mb: GroupVD): GroupVD = {
    GroupVD(ma.accept ++ mb.accept, ma.sent ++ mb.sent, ma.ids ++ mb.ids)
  }

}

case class MsgScore(groupId: Long, from: Long, to: Long, score: Double) extends Serializable

case class GroupVD(accept: Set[MsgScore], sent: Set[MsgScore], ids: Set[Long]) extends Serializable

//typ 1:touzi 2:kongzhi
case class MemRelBreadth(from: Long, to: Long, score: Double,groupId: Long,fromlevel:Double,tolevel:Double) extends Serializable{
  override def toString: String = from + "#" + to +"#"+groupId+"#"+fromlevel+"#"+tolevel
}

case class MemRelBreadth2(groupId: Long,path:String, ids: Set[Long]) extends Serializable{
  override def toString: String =groupId+"#"+path
}

case class MemRelBreadth3(from: Long, to: Long, score: Double,groupId: Long,path:String) extends Serializable{
  override def toString: String =groupId+"#"+path
}

case class MVD(from: Long, to: Long, score: Double, ids: Set[Long])  extends Serializable

case class MemLevel(id:Long,groupId: Long,level:Long)  extends Serializable

case class MemIds(from: Long, to: Long,groupId: Long)  extends Serializable {
  override def toString: String = from + "#" + to +"#"+groupId
}

case class MemIds2(from: Long,gids: Set[Long],path:String)  extends Serializable {
  override def toString: String = from + "#" + path
}
case class MemRel2(from: Long, to: Long, score: Double) extends Serializable{
  override def toString: String = from + "#" + to +"#"+score
}
case class GroupMem2(targetId: Long, groupId: Long,rel: Set[MemRel2]) extends Serializable

case class GroupMem3(targetId: Long,rel: Set[MemRel2],gids: Set[Long],paths:Set[String]) extends Serializable


case class GVD(from: Long, groupId: Long, score: Double, rel: Set[MemRel2])  extends Serializable