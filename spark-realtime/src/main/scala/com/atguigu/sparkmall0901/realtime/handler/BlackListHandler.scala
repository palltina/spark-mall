package com.atguigu.sparkmall0901.realtime.handler

import java.util
import java.util.Properties

import com.atguigu.sparkmall.commen.util.{PropertiesUtil, RedisUtil}
import com.atguigu.sparkmall0901.realtime.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {
	def handle(adsLogDstream: DStream[AdsLog])={
		val clickCountperAdsPerDayDstream: DStream[(String, Long)] = adsLogDstream.map{adsLog => (adsLog.getDate()+"_"+adsLog.userId+"_"+adsLog.adsId,1L)}
		clickCountperAdsPerDayDstream.foreachRDD(rdd => {
			val prop: Properties = PropertiesUtil.load("config.properties")
			rdd.foreachPartition{adsItr=>
				val jedis = RedisUtil.getJedisClient
				adsItr.foreach{case (logkey,count) =>
					val day: String = logkey.split("_")(0)
					val user: String = logkey.split("_")(1)
					val ads: String = logkey.split("_")(2)
					val key = "user_ads_click:" + day
					jedis.hincrBy(key,user+"_"+ads,count)
					val curCount: String = jedis.hget(key,user+"_"+ads)
					if(curCount.toLong>=100){
						jedis.sadd("blacklist",user)
					}
				}
				jedis.close()
			}
		})
	}
	def check(sparkContext: SparkContext,adsLongDstream:DStream[AdsLog])={
		val filteredAdsLogDstream: DStream[AdsLog] = adsLongDstream.transform { rdd =>
			val prop: Properties = PropertiesUtil.load("config.properties")
			val jedis: Jedis = RedisUtil.getJedisClient
			val blacklistSet: util.Set[String] = jedis.smembers("blacklist")
			val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blacklistSet)
			rdd.filter { adsLog =>
				!blacklistBC.value.contains(adsLog.userId)
			}
		}
		filteredAdsLogDstream
	}
}
