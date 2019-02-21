package com.atguigu.sparkmall.offline.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

class CityRatioUDAF extends UserDefinedAggregateFunction{
	override def inputSchema: StructType = StructType(Array(StructField("city_name",StringType)))

	override def bufferSchema: StructType = StructType(Array(StructField("city_count",MapType(StringType,LongType)),StructField("total_count",LongType)))

	override def dataType: DataType = StringType

	override def deterministic: Boolean = true

	override def initialize(buffer: MutableAggregationBuffer): Unit = {
		buffer(0) = new HashMap[String,Long]()
		buffer(1) = 0L
	}

	override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		val cityCountMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
		val totalCount: Long = buffer.getLong(1)
		val cityName: String = input.getString(0)
		buffer(0) = cityCountMap + (cityName ->( cityCountMap.getOrElse(cityName,0L) + 1L))
		buffer(1) = totalCount + 1L
	}

	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
		val cityCountMap1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
		val totalCount1: Long = buffer1.getLong(1)
		val cityCountMap2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)
		val totalCount2: Long = buffer2.getLong(1)
		buffer1(0) = cityCountMap1.foldLeft(cityCountMap2){case(cityCountMap2,(cityName1,count1))=>
			cityCountMap2 + (cityName1 -> (cityCountMap2.getOrElse(cityName1,0L) + count1))
		}
		buffer1(1) = totalCount1 + totalCount2
	}

	override def evaluate(buffer: Row): Any = {
		val cityCountMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
		val totalCount: Long = buffer.getLong(1)
		var cityRatioInfoList: List[CityRatioInfo] = cityCountMap.map { case (cityName, count) =>
			val cityRatio: Double = math.round(count.toDouble / totalCount * 1000) / 10D
			CityRatioInfo(cityName, cityRatio)
		}.toList
		var cityRatioInfoTop2List: List[CityRatioInfo] = cityRatioInfoList.sortBy(_.cityRatio)(Ordering.Double.reverse).take(2)
		if(cityRatioInfoList.size > 2){
			var otherRatio = 100D
			cityRatioInfoTop2List.foreach(cityRatioInfo => otherRatio-=cityRatioInfo.cityRatio)
			otherRatio = math.round(otherRatio*10)/10D
			cityRatioInfoTop2List = cityRatioInfoTop2List :+ CityRatioInfo("其他",otherRatio)
		}
		cityRatioInfoTop2List.mkString(",")
	}
}
case class CityRatioInfo(cityName: String, cityRatio: Double) {
	override def toString: String = {
		cityName + ":" + cityRatio + "%"
	}
}