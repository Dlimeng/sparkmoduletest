package com.lm.spark.model

import java.util.Date

import java.sql.Timestamp

/**
  * Created by Songyuandi on 2019/12/12
  */

case class Node(strId: String, name: String, var longId: Long, isPerson: Boolean, regcap: Double = 0D, entStatus: String) extends Serializable

case class NodeCP(strId: String, name: String, var longId: Long, regcap: Double) extends Serializable {
  override def toString: String = strId + "#" + longId + "#" + name
}

case class MsgScore(groupId: Long, from: Long, to: Long, score: Double) extends Serializable

//case class GroupMsg(accept: Set[MsgScore], sent: Set[MsgScore], ids: Set[Long]) extends Serializable

case class GroupVD(accept: Set[MsgScore], sent: Set[MsgScore], ids: Set[Long], isListed: Boolean) extends Serializable

//memType: 1:集团成员 2:疑似集团成员
case class GroupMem(targetId: Long, groupId: Long, score: Double,
                    memType: Int, tag: Int = 0, rel: Set[MemRel]) extends Serializable

//typ 1:touzi 2:kongzhi
case class MemRel(from: Long, to: Long, score: Double, typ: Int) extends Serializable{
  override def toString: String = from + "#" + to
}

case class GroupOut(targetId: Long, groupId: Long, score: Double, memType: Int, tag: Int,
                    topCompanyRegCapital: Double, topCompanyNum: Int, regCapitalSum: Double) extends Serializable

case class Relation(fromId: Long, toId: Long, score: Double, fromRegcap: Double, toRegcap: Double) extends  Serializable

case class RelationInfo(id: String, fromID: String, fromName: String, fTag: String, fType: String,
                        toID: String, toName: String, tTag: String, tType: String,
                        relationType: String, score: Double, title: String, grouId: String) extends Serializable

case class NodeWithTag(groupid: Long, longId: Long, regcap: Double, tag: Int, memType: Int, memTag: Int, score: Double, rel: Set[MemRel]) extends  Serializable

//写入es 的集团成员间关系
case class ESGroupRe(id: String, from: String, to: String, relation_type: String, score: Double, title: String) extends Serializable

//写入es 的集团成员间节点
case class ESGroupNode(id: String, name: String, tag: String, types: String) extends Serializable {

  override def hashCode(): Int = this.id.hashCode()

  override def equals(obj: scala.Any): Boolean = {
    if (obj == null) false else {
      val o = obj.asInstanceOf[ESGroupNode]
      o.id.equals(this.id)
    }
  }

}

//写入ES集团成员
case class InsertEsGroup(source_id: String, links: List[ESGroupRe], nodes: Seq[ESGroupNode], group_reg_capital: String,
                         group_num: String, reg_capital_sum: String) extends Serializable

case class FinalInsertEsGroup(source_id: String, group_name: String, show_group_name: String, links: List[ESGroupRe], nodes: Set[ESGroupNode], group_reg_capital: String,
                              group_num: String, reg_capital_sum: String) extends Serializable



case class EsSearch(id: String, name: String, group_id: String, dept_id: String, group_name: String, show_group_name: String,
                    group_reg_capital: String, group_num: String, reg_capital_sum: String) extends Serializable


case class MysqlWhiteList(id: String, groupId: String, entid: String, showName: String,
                          entname: String, state: String, tType: String,
                          createDate: Timestamp, updateDate: Timestamp, createBy: String, createByName: String) extends  Serializable

case class SimpleGroup(entid: String, groupId: String) extends Serializable

case class EsSearchEnt(id: String, search_name: String, entstatus: String, regcap: Double, entcategory: String, esdate: String,
                       nodenum: String, industryphy: String, is_group: String) extends Serializable