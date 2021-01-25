package com.lm.graphx.pojo

/**
  * @Classname CaseModel
  * @Description TODO
  * @Date 2020/6/16 18:32
  * @Created by limeng
  */
class CaseModel {

}
case class FromInfo(srcId:Long,score:Double, root: Boolean,path:String) extends Serializable{
  override def hashCode(): Int = srcId.hashCode()

  override def equals(obj: Any): Boolean = {
    if(obj == null) false else{
      val o = obj.asInstanceOf[FromInfo]
      o.srcId.equals(this.srcId)
    }
  }

  override def toString: String = srcId+"#"+ root.toString+"#"+path +"#"+score.formatted("%.3f")
}

case class InAndOut(in:List[FromInfo],out:List[FromInfo]) extends Serializable

// flag 1 给OUT  0 给In
case class MsgFlag(srcId: Long, score: Double, flag: Int, root: Boolean,path:String) extends Serializable {
  override def toString: String = srcId + " # " + flag
}



case class MsgScore(groupId: Long, from: Long, to: Long, score: Double) extends Serializable {
  override def toString: String = s"$groupId#$from#$to#$score"
}

case class GroupVD(accept: Set[MsgScore], sent: Set[MsgScore], ids: Set[Long], isListed: Boolean) extends Serializable

//发送给目标节点消息
case class MsgToDsc(srcId: Long, score: Double) extends Serializable


object NodeOrdering extends Ordering[MsgToDsc] {
  override def compare(x: MsgToDsc, y: MsgToDsc): Int = {
      x.score.compareTo(y.score)
  }
}