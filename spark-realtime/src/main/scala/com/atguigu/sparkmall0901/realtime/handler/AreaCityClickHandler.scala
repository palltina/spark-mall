package com.atguigu.sparkmall0901.realtime.handler

import com.atguigu.sparkmall.commen.util.{PropertiesUtil, RedisUtil}
import com.atguigu.sparkmall0901.realtime.bean.AdsLog
import org.apache.spark.streaming.dstream.DStream

object AreaCityClickHandler {
	def handle(adsLogDstream: DStream[AdsLog])={
		val adsClickDstream: DStream[(String, Long)] = adsLogDstream.map { adsLog =>
			val key: String = adsLog.getDate() + ":" + adsLog.area + ":" + adsLog.city + ":" + adsLog.adsId
			(key, 1L)
		}
		val adsClickCountDstream: DStream[(String, Long)] = adsClickDstream.updateStateByKey { (countSeq: Seq[Long], total: Option[Long]) =>
			val countSum: Long = countSeq.sum
			val curTotal: Long = total.getOrElse(0L) + countSum
			Some(curTotal)
		}
		adsClickCountDstream.foreachRDD { rdd =>
			val prop: Unit = PropertiesUtil.load("config.properties")
			rdd.foreachPartition { adsClickCountItr =>
				val jedis = RedisUtil.getJedisClient
				adsClickCountItr.foreach { case (key, count) =>
					jedis.hset("date:area:city:ads", key, count.toString)
				}
				jedis.close()
			}

		}
		adsClickCountDstream
	}
}
