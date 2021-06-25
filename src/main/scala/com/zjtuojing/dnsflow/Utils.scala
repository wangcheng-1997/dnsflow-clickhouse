package com.zjtuojing.dnsflow

import java.io.{BufferedInputStream, File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.regex.Pattern

import com.zjtuojing.utils.{Constant, DNSUtils}
import kafka.common.TopicAndPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
 * ClassName MySQLUtils
 * Date 2019/7/15 15:07
 **/
object Utils {

  val properties = DNSUtils.loadConf()


  def apply() = {
    DBs.setup()
  }

  def main(args: Array[String]): Unit = {
    val jedisPool = JedisPoolUser.getJedisPool()
    val jedis = JedisPoolUser.getJedisClient(jedisPool)
    val username = DnsRptCH.getUserName(jedis)
    println(username.getOrElse("100.104.103.190",""))
  }

  /**
   * 根据groupId查询偏移量数据，并将结果封装成Map[TopicAndPartition, Long]
   */
  def getOffsetsBy(groupId: String): Map[TopicAndPartition, Long] = {

    DB.readOnly { implicit session =>
      SQL("select * from kafka_offset where groupId = ?")
        .bind(groupId)
        .map(rs => (TopicAndPartition(rs.string("topic"), rs.int("partitionNum")), rs.long("offsets")))
        .list().apply()
    }.toMap

  }


  /**
   * 时间戳转换为date
   */
  def timestamp2Date(pattern: String, timestamp: Long): String = {
    val time: String = new SimpleDateFormat(pattern).format(timestamp)
    time
  }


  /**
   * 获取权威域名
   */
  def domian2Authority(domain: String): String = {
    //权威域名正则
    val pDomain = Pattern.compile(Constant.REGULAR_DOMAIN)
    var authorityDomain = domain
    try {
      val mDomain = pDomain.matcher(domain)
      while (mDomain.find()) {
        authorityDomain = mDomain.group()
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    authorityDomain
  }

  /**
   * IP转Long类型
   */
  def ipToLong(ip: String): Long = {
    val arr = ip.split("\\.")
    var ip2long = 0L
    if (arr.length == 4) {
      var i = 0
      while ( {
        i < 4
      }) {
        ip2long = ip2long << 8 | arr(i).toInt

        {
          i += 1;
          i
        }
      }
    }
    ip2long
  }


  /**
   * 读取mysql数据
   */
  def ReadMysql(spark: SQLContext, tableName: String): DataFrame = {

    val df = spark.read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("dbtable", tableName)
      .load()

    df
  }


  /**
   * 写入MySQL
   */
  def Write2Mysql(df: DataFrame, tableName: String) = {
    df.write.mode(SaveMode.Overwrite)
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("dbtable", tableName)
  }

}
