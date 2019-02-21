package com.atguigu.sparkmall.offline.app

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.commen.bean.UserVisitAction
import com.atguigu.sparkmall.commen.util.PropertiesUtil
import com.atguigu.sparkmall.offline.bean.CategoryCount
import com.atguigu.sparkmall.offline.handler.{CategoryTop10Handler, Top10CategoryTop10SessionHandler}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OfflineApp {
	def main(args: Array[String]): Unit = {
		val taskId = UUID.randomUUID().toString
		val sparkConf: SparkConf = new SparkConf().setAppName("offline").setMaster("local[*]")
		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
		val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionToRDD(sparkSession)
		val top10CategoryList: List[CategoryCount] = CategoryTop10Handler.handle(sparkSession,userVisitActionRDD,taskId)
		println("需求一完成")
		Top10CategoryTop10SessionHandler.handle(taskId,sparkSession,userVisitActionRDD,top10CategoryList)
		println("需求二完成")
	}

	def readUserVisitActionToRDD(sparkSession: SparkSession)={


		val sql = new StringBuilder("select v.* from user_visit_action v ,user_info u where v.user_id = u.user_id")

		val prop = PropertiesUtil.load("conditions.properties")
		val conditionJson: String = prop.getProperty("condition.params.json")
		val conditionJsonObj = JSON.parseObject(conditionJson)
		val startDate: String = conditionJsonObj.getString("startDate")
		val endDate: String = conditionJsonObj.getString("endDate")
		val startAge: String = conditionJsonObj.getString("startAge")
		val endAge: String = conditionJsonObj.getString("endAge")

		if(startDate.nonEmpty){
			sql.append(" and date >= '" + startDate + "'")
		}
		if(endDate.nonEmpty){
			sql.append(" and date <= '" + endDate + "'")
		}
		if(startAge.nonEmpty){
			sql.append(" and age >=" + startAge)
		}
		if(endAge.nonEmpty){
			sql.append(" and age <=" + endAge)
		}
		println(sql)
		sparkSession.sql("use sparkmall0901")
		import sparkSession.implicits._
		val rdd = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd
		rdd
	}
}
