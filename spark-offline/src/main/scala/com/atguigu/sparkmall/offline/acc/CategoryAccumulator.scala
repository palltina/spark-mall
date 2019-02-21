package com.atguigu.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
	var categoryCountMap = new mutable.HashMap[String,Long]()
	override def isZero: Boolean = categoryCountMap.isEmpty

	override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = new CategoryAccumulator

	override def reset(): Unit = categoryCountMap.clear()

	override def add(v: String): Unit = {
		categoryCountMap(v) = categoryCountMap.getOrElse(v,0L) + 1L
	}

	override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
		val otherMap: mutable.HashMap[String, Long] = other.value
		categoryCountMap = categoryCountMap.foldLeft(otherMap){case (otherMap,(key,count))=>
				otherMap(key) = otherMap.getOrElse(key,0L) + count
				otherMap
		}
	}

	override def value: mutable.HashMap[String, Long] = categoryCountMap
}
