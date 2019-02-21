package com.atguigu.sparkmall.offline.handler

import com.atguigu.sparkmall.commen.bean.UserVisitAction
import com.atguigu.sparkmall.commen.util.JdbcUtil
import com.atguigu.sparkmall.offline.acc.CategoryAccumulator
import com.atguigu.sparkmall.offline.bean.CategoryCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryTop10Handler {
	def handle(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction],taskId:String)={
		val accumulator = new CategoryAccumulator
		sparkSession.sparkContext.register(accumulator)
		userVisitActionRDD.foreach{userVisitAction=>
			if(userVisitAction.click_category_id != -1L){
				val key: String = userVisitAction.click_category_id + "_click"
				accumulator.add(key)
			}else if(userVisitAction.order_category_ids != null && userVisitAction.order_category_ids.length>0){
				val orderCids: Array[String] = userVisitAction.order_category_ids.split(",")
				for (elem <- orderCids) {
					val key: String = elem + "_order"
					accumulator.add(key)
				}
			}else if(userVisitAction.pay_category_ids != null && userVisitAction.pay_category_ids.length>0){
				val payCids: Array[String] = userVisitAction.pay_category_ids.split(",")
				for (elem <- payCids) {
					val key: String = elem + "_pay"
					accumulator.add(key)
				}
			}
		}
		val categoryMap: mutable.HashMap[String, Long] = accumulator.value
		println(s"categoryMap = ${categoryMap.mkString("\n")}")
		val categoryGroupbyCidMap: Map[String, mutable.HashMap[String, Long]] = categoryMap.groupBy{case (key,count)=> key.split("_")(0)}
		println(s"categoryGroupbyCidMap = ${categoryGroupbyCidMap.mkString("\n")}")
		val categoryCountList: List[CategoryCount] = categoryGroupbyCidMap.map { case (cid, actionMap) =>
			CategoryCount("", cid, actionMap.getOrElse(cid + "_click", 0L), actionMap.getOrElse(cid + "_order", 0L), actionMap.getOrElse(cid + "_pay", 0L))
		}.toList
		val sortedCategoryCountList: List[CategoryCount] = categoryCountList.sortWith { (categoryCount1, categoryCount2) =>
			if (categoryCount1.clickCount > categoryCount2.clickCount) {
				true
			} else if (categoryCount1.clickCount == categoryCount2.clickCount) {
				if (categoryCount1.orderCount > categoryCount2.orderCount) {
					true
				} else if (categoryCount1.orderCount == categoryCount2.orderCount) {
					if (categoryCount1.payCount > categoryCount2.payCount) {
						true
					} else {
						false
					}
				} else {
					false
				}
			} else {
				false
			}
		}.take(10)
		val resultList: List[Array[Any]] = sortedCategoryCountList.map{categoryCount => Array(taskId,categoryCount.categoryId,categoryCount.clickCount,categoryCount.orderCount,categoryCount.payCount)}
		JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",resultList)
		sortedCategoryCountList
	}
}
