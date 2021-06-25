package com.zjtuojing.dnsflow

import com.zjtuojing.utils.DNSUtils
import redis.clients.jedis.{Jedis, JedisPool}

object JedisPool {

  val properties = DNSUtils.loadConf()
  val password = properties.getProperty("redis.password")

  val TIME_OUT = 30000

  def getJedisPool(): JedisPool = {

    val redisHost = properties.getProperty("redis.host")
    val redisPort = properties.getProperty("redis.port")

    val pool = new JedisPool(redisHost,redisPort.toInt)

    pool
  }

  def getJedisClient(pool: JedisPool): Jedis = {
    val jedis = pool.getResource
    jedis.auth(password)
    jedis
  }

  def getJedisClient(): Jedis = {
    val redisHost = properties.getProperty("redis.host")
    val redisPort = properties.getProperty("redis.port")

    val pool = new JedisPool(redisHost,redisPort.toInt)
    val jedis = pool.getResource
    jedis.auth(password)
    jedis
  }

  def main(args: Array[String]): Unit = {

    val jedisPool = getJedisPool()
    val jedis = getJedisClient(jedisPool)
    jedis.hgetAll("dns:dnsIpSegment")
      .values().toArray
      .take(1).foreach(println(_))


    jedis.close()
  }


}
