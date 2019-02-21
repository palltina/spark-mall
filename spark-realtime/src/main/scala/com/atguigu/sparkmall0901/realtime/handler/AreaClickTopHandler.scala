package com.atguigu.sparkmall0901.realtime.handler

import com.atguigu.sparkmall.commen.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._

object AreaClickTopHandler {
	def handle(areaCityClickDstream: DStream[(String, Long)])={
		val areaAdsCountDstream: DStream[(String, Long)] = areaCityClickDstream.map { case (day_area_city_ads, count) =>
			val day: String = day_area_city_ads.split(":")(0)
			val area: String = day_area_city_ads.split(":")(1)
			val city: String = day_area_city_ads.split(":")(2)
			val adsId: String = day_area_city_ads.split(":")(3)
			(day + ":" + area + ":" + adsId, count)
		}.reduceByKey(_ + _)
		val adsCountGroupbyAreaDstream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsCountDstream.map { case (day_area_ads, count) =>
			val day: String = day_area_ads.split(":")(0)
			val area: String = day_area_ads.split(":")(1)
			val adsId: String = day_area_ads.split(":")(2)
			(day, (area, (adsId, count)))
		}.groupByKey()
		val areaAdsTop3JsonCountGroupbyDayDstream: DStream[(String, Map[String, String])] = adsCountGroupbyAreaDstream.map { case (day, areaItr) =>
			val adsCountGroupbyAreaMap: Map[String, Iterable[(String, (String, Long))]] = areaItr.groupBy { case (area, (ads, count)) => area }
			val adsTop3CountGroupbyAreaMap: Map[String, String] = adsCountGroupbyAreaMap.map { case (area, adsItr) =>
				val top3AdsList: List[(String, Long)] = adsItr.map { case (area, (adsId, count)) => (adsId, count) }.toList.sortWith(_._2 > _._2).take(3)
				val top3AdsJsonString: String = JsonMethods.compact(JsonMethods.render(top3AdsList))
				(area, top3AdsJsonString)
			}
			(day, adsTop3CountGroupbyAreaMap)
		}
		areaAdsTop3JsonCountGroupbyDayDstream.foreachRDD{rdd =>
			rdd.foreachPartition{dayItr =>
				val jedis = RedisUtil.getJedisClient
				dayItr.foreach{case (day,areaMap)=>
						import collection.JavaConversions._
						jedis.hmset("area_top3_ads:" + day,areaMap)
				}
				jedis.close()

			}


		}

	}
}
