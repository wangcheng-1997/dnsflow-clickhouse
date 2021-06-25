package com.zjtuojing.utils

import java.sql.SQLFeatureNotSupportedException
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ClickUtils {
  val prop = DNSUtils.loadConf()
  val url = prop.getProperty("clickhouse.url")
  val user = prop.getProperty("clickhouse.user")
  val password = prop.getProperty("clickhouse.password")

  val ch_prop = new Properties()
  ch_prop.put("driver", "com.github.housepower.jdbc.ClickHouseDriver")
  //  prop.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  ch_prop.put("user", user)
  ch_prop.put("password", password)


  def clickhouseWrite(df: DataFrame, table: String): Unit = {
    try {
      df.write.mode(saveMode = "append")
        .option("batchsize", "100000")
        .option("isolationLevel", "NONE") // 设置事务
        .option("numPartitions", "1") // 设置并发
        .jdbc(url, table, ch_prop)
    } catch {
      case e: SQLFeatureNotSupportedException =>
        println("catch and ignore!")
    }
  }

  def main(args: Array[String]): Unit = {
    //    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //    val time = 1616630400000L
    //    val str = format.format(time)
    //    val date: Date = format.parse(str)
    //    println(str,date)
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val prop = new Properties()
    //    prop.put("driver", "com.github.housepower.jdbc.ClickHouseDriver")
    //    //  prop.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    //    prop.put("user", "tuojing")
    //    prop.put("password", "123456")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val frame = spark.read
      .format("jdbc")
      .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
      .option("url", url)

      .option("dbtable", "dns_flow.dns_flow_clear")
      .load()

    import org.apache.spark.sql.functions._
    frame.orderBy(-col("resolver")).where("accesstime=1616631060").show()
  }
}
