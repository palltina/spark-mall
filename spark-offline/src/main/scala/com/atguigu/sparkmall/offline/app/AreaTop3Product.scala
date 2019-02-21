package com.atguigu.sparkmall.offline.app

import java.util.Properties

import com.atguigu.sparkmall.commen.util.PropertiesUtil
import com.atguigu.sparkmall.offline.udf.CityRatioUDAF
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object AreaTop3Product {
	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setAppName("areaTop3Product").setMaster("local[*]")
		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
		sparkSession.udf.register("city_ratio",new CityRatioUDAF)
		val prop: Properties = PropertiesUtil.load("config.properties")
		sparkSession.sql("use sparkmall0901")
		sparkSession.sql("select ci.city_name,ci.area,click_product_id from user_visit_action uv inner join city_info ci where uv.city_id = ci.city_id and click_product_id > 0").createOrReplaceTempView("areaCityClickCount" )
		sparkSession.sql("select area,click_product_id,count(*) clickcount,city_ratio(city_name) city_remark from areaCityClickCount group by area,click_product_id").createOrReplaceTempView("clickCountGroupbyAreaPid")
		sparkSession.sql("select area,click_product_id,clickcount,city_remark from (select v.*,row_number()over(partition by area order by clickcount desc) rk from clickCountGroupbyAreaPid v) clickrk where rk <= 3").createOrReplaceTempView("areaProductTop3")
		sparkSession.sql("select area,p.product_name,clickcount,city_remark from areaProductTop3 v ,product_info p where v.click_product_id=p.product_id").write
    		.format("jdbc").option("url",prop.getProperty("jdbc.url"))
    		.option("user",prop.getProperty("jdbc.user"))
    		.option("password",prop.getProperty("jdbc.password"))
    		.option("dbtable","area_count_info").mode(SaveMode.Append).save()

	}
}
