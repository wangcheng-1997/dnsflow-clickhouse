package com.zjtuojing.utils

import java.io.FileInputStream
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Date, Properties}


/**
 * ClassName DNSUtils
 * Date 2019/3/5 14:08
 **/
object DNSUtils {
  def getTaskTime = {
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH")
    val df2 = new SimpleDateFormat("yyyy-MM-dd")
    val df3 = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val now = new Date()
    val time = df1.format(now.getTime)
    val time1 = df1.parse(time).getTime / 1000

    val now_hour = time.substring(11, 13).toInt
    val time2 = df1.parse(time.replaceAll("\\s+\\d{2}", s" ${now_hour / 6 * 6}")).getTime / 1000

    val time3 = df2.parse(df2.format(now.getTime)).getTime / 1000

    val timem = df3.format(now)
    val now_min = timem.substring(14, 16).toInt
    val time4 = df3.parse(timem.replaceAll(":[0-5][0-9]", s":${now_min / 5 * 5}")).getTime / 1000

    val hour: Long = time1 - 3600
    val sixHour: Long = time2 - 21600
    val date: Long = time3 - 86400
    val minute: Long = time4 - 300

    ((time1, hour), (time2, sixHour), (time3, date), (time4, minute))
  }

  def longToIp(i: Long) = ((i >> 24) & 0xFF) + "." + ((i >> 16) & 0xFF) + "." + ((i >> 8) & 0xFF) + "." + (i & 0xFF)

  def getNewData(): String = {

    val now: Date = new Date()
    val dayTime = now.getTime - 86400000
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date = dateFormat.format(new Date(dayTime))
    date
  }

  def newDate = {
    val df2: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    df2.format(new Date())
  }


  def MD5Encode(input: String): String = {

    // 指定MD5加密算法
    val md5: MessageDigest = MessageDigest.getInstance("MD5")

    // 对输入数据进行加密,过程是先将字符串中转换成byte数组,然后进行随机哈希

    val encoded: Array[Byte] = md5.digest(input.getBytes)

    // 将加密后的每个字节转化成十六进制，一个字节8位，相当于2个16进制，不足2位的前面补0
    encoded.map("%02x".format(_)).mkString
  }

  def loadConf(): Properties ={

    val properties = new Properties
    val ipstream = new FileInputStream("config.properties")
    properties.load(ipstream)
    properties
  }

  def main(args: Array[String]): Unit = {

    println(getTaskTime)

    println(getNewData())

    println(getNewData().substring(0, 4))


  }

}
