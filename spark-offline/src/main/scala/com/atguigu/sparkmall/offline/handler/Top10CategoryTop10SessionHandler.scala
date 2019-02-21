package com.atguigu.sparkmall.offline.handler

import com.atguigu.sparkmall.commen.bean.UserVisitAction
import com.atguigu.sparkmall.commen.util.JdbcUtil
import com.atguigu.sparkmall.offline.bean.CategoryCount
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcUnion
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Top10CategoryTop10SessionHandler {
	def handle(taskId:String,sparkSession: SparkSession,userVisitActionRDD: RDD[UserVisitAction],top10CategoryList: List[CategoryCount])={
		val cidTop10: List[Long] = top10CategoryList.map(_.categoryId.toLong)
		val cidTop10BC: Broadcast[List[Long]] = sparkSession.sparkContext.broadcast(cidTop10)
		val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction =>
			cidTop10BC.value.contains(userVisitAction.click_category_id)
		}
		val clickCountGroupbyCidSessionRDD: RDD[(String, Long)] = filteredUserVisitActionRDD.map(action =>
			(action.click_category_id + "_" + action.session_id, 1L)).reduceByKey(_ + _)
		val sessionCountGroupbyCidRDD: RDD[(String, Iterable[(String, Long)])] = clickCountGroupbyCidSessionRDD.map { case (cidSession, count) =>
			val cid: String = cidSession.split("_")(0)
			val sessionId: String = cidSession.split("_")(1)
			(cid, (sessionId, count))
		}.groupByKey()
		val resultRDD: RDD[Array[Any]] = sessionCountGroupbyCidRDD.flatMap { case (cid, sessionItr) =>
			val top10sessionList: List[(String, Long)] = sessionItr.toList.sortWith { (sessionCount1, sessionCount2) =>
				sessionCount1._2 > sessionCount2._2
			}.take(10)
			val sessonTop10ListWithCidList: List[Array[Any]] = top10sessionList.map { case (sessionId, count) =>
				Array(taskId, cid, sessionId, count)
			}
			sessonTop10ListWithCidList
		}
		val resultArray: Array[Array[Any]] = resultRDD.collect()
		JdbcUtil.executeBatchUpdate("insert into category_top10_session_top10 values (?,?,?,?)",resultArray)
	}
}
