package com.zjtuojing.dnsflow

import java.util

import com.zjtuojing.utils.DNSUtils
import redis.clients.jedis.{Jedis, JedisPoolConfig, JedisSentinelPool}

object JedisPoolUser {

  val properties = DNSUtils.loadConf()

  val MASTER_NAME = "mymaster"
  val TIME_OUT = 150000

  def getJedisPool(): JedisSentinelPool = {

    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setTestOnBorrow(true)
    jedisPoolConfig.setTestOnReturn(true)
    jedisPoolConfig.setTestWhileIdle(true)
    jedisPoolConfig.setMaxTotal(-1)

    val sentinelSet = new util.HashSet[String]()
    val redisUrl = properties.getProperty("redis.url")
    val urls: Array[String] = redisUrl.split(",")
    for (i <- urls.indices) {
      sentinelSet.add(urls(i))
    }

    val password = properties.getProperty("redis.password")

    val pool = new JedisSentinelPool(MASTER_NAME, sentinelSet, jedisPoolConfig, TIME_OUT, password)

    pool
  }

  def getJedisClient(pool: JedisSentinelPool): Jedis = {
    val jedis = pool.getResource
    jedis
  }

  def getJedisClient(): Jedis = {
    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setTestOnBorrow(true)
    jedisPoolConfig.setTestOnReturn(true)
    jedisPoolConfig.setTestWhileIdle(true)
    jedisPoolConfig.setMaxTotal(-1)

    val sentinelSet = new util.HashSet[String]()
    val redisUrl = properties.getProperty("redis.url")
    val urls: Array[String] = redisUrl.split(",")
    for (i <- urls.indices) {
      sentinelSet.add(urls(i))
    }

    val password = properties.getProperty("redis.password")

    val pool = new JedisSentinelPool(MASTER_NAME, sentinelSet, jedisPoolConfig, TIME_OUT, password)
    val jedis = pool.getResource
    jedis
  }

  def main(args: Array[String]): Unit = {

    val jedisPool = getJedisPool()
    val jedis = getJedisClient(jedisPool)
    jedis.hgetAll("broadband:userinfo")
      .values().toArray
      .take(1).foreach(println(_))


    jedis.close()
  }


}
