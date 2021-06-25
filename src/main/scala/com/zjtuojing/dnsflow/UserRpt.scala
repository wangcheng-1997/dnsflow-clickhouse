package com.zjtuojing.dnsflow

import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.zjtuojing.utils.DNSUtils
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
 * ClassName UserRpt
 * Date 2019/12/30 10:53
 **/
object UserRpt {

  val properties = DNSUtils.loadConf()


  def getRedisAuthDomainTags(jedis: Jedis, index: String): Map[String, String] = {
    //读取Redis基础表数据
    val maps = jedis.hgetAll(index)
      .values()
      .toArray()
      .map(json => {
        val jobj = JSON.parseObject(json.toString)
        val authDomain = jobj.getString("authDomain")
        val websiteName = jobj.getString("websiteName")
        (authDomain, websiteName)
      }).toMap
    maps
  }

  def getRedisDoaminTags(jedis: Jedis, index: String): Map[String, String] = {
    //读取Redis数据
    val domainMaps = jedis.hgetAll(index)
      .values()
      .toArray()
      .map(json => {
        val jobj = JSON.parseObject(json.toString)
        val domain = jobj.getString("domain")
        val oneLabel = jobj.getString("oneLabel")
        val twoLabel = jobj.getString("twoLabel")
        val threeLabel = jobj.getString("threeLabel")
        (domain, oneLabel + "|" + twoLabel + "|" + threeLabel)
      }).toMap
    domainMaps
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println(
        """
          |参数错误：
          |com.zjtuojing.dnsflow.UserRpt
          |
          |<日志时间>
        """.stripMargin)
      sys.exit()
    }

    val Array(yesterday) = args

    //创建SparkContext实例对象
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
//    conf.setMaster("local[4]")
      .set("es.port", properties.getProperty("es2.port"))
      .set("es.nodes", properties.getProperty("es2.nodes"))
      .set("es.nodes.wan.only", properties.getProperty("es2.nodes.wan.only"))
      .set("es.index.auto.create", properties.getProperty("es2.index.auto.create"))
    //设置写入es参数
    val sc = SparkContext.getOrCreate(conf)
    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices",properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1",properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    println(properties.getProperty("fs.defaultFS"))

//    //TODO 整理原始数据
//    val userBaseRDD = sc.textFile(s"hdfs://nns/dns_middle_data/${yesterday}/*/*")
//      //val userBaseRDD = sc.textFile(s"hdfs://nns/dns_middle_data/${yesterday}/202002190000/part-00000")
//      .map(line => {
//        var userName = ""
//        var domain = ""
//        var num = 1L
//        val files = line.split("\001")
//        try {
//          userName = files(0)
//          domain = files(1)
//          num = files(2).toLong
//        } catch {
//          case e: Exception => {
//            //e.printStackTrace()
//          }
//        }
//        UserRptBean(userName, domain, num)
//      }).persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//
//    //日志时间
//    val accesstime = new SimpleDateFormat("yyyyMMdd").parse(yesterday).getTime
//
//    //TODO 计算用户关联域名Top10
//    val topList = userBaseRDD.map(tuple => ((tuple.userName, tuple.domain), tuple.resolver))
//      .filter(!_._1._2.contains("null"))
//      .reduceByKey(_ + _)
//      .groupBy(_._1._1)
//      .mapValues(itr => {
//        itr.toList.sortBy(_._2).reverse.take(10)
//      })
//
//    //用户关联域名Top10写入ES
//    val topBean = topList.map(line => {
//      var domianAndResolve = Map[String, Any]()
//      var buff = new ArrayBuffer[Map[String, Any]]()
//      val userAndDomainResolve = line._2
//      for (index <- 0 to userAndDomainResolve.size - 1) {
//        domianAndResolve += ("domain" -> userAndDomainResolve(index)._1._2)
//        domianAndResolve += ("count" -> userAndDomainResolve(index)._2)
//        buff += domianAndResolve
//      }
//      UserDomainTop10("dnsflow", "topN", line._1, buff, accesstime)
//    })
//    EsSpark.saveToEs(topBean, s"bigdata_dns_flowmsg_userinfo_${yesterday.substring(0, 6)}/report")
//
//
//    //TODO 计算用户全天解析总次数
//    val userResolvers = userBaseRDD.map(tuple => (tuple.userName, tuple.resolver))
//      .reduceByKey(_ + _)
//      .map(reduced => UserAllResolver("dnsflow", "userResolvers", reduced._1, reduced._2, accesstime))
//    EsSpark.saveToEs(userResolvers, s"bigdata_dns_flowmsg_userinfo_${yesterday.substring(0, 6)}/report")
//
//
//    //TODO 给数据打标签
//    //数据去重
//    val distincted = userBaseRDD.map(line => (line.userName, line.domain)).distinct()
//    //读取redis规则数据
//    val jedis = new Jedis("30.250.11.158", 6399, 3000)
//    //把规则表和基础表广播出去
//    val tags = getRedisDoaminTags(jedis, "key-cache-liveness-domain-label")
//    val broadcastTags = sc.broadcast(tags)
//    //val baseTags = getRedisAuthDomainTags(jedis, "broadband-probe-cache-dns-authDomain")
//    //val broadcastBaseTags = sc.broadcast(baseTags)
//
//    //去重后的数据匹配规则标签
//    val userAndDomainTags = distincted.map(tuple => {
//      val broadcastMaps = broadcastTags.value
//      //打标签
//      val tagFiles = broadcastMaps.getOrElse(tuple._2, "未知|未知|未知").split("\\|")
//      var oneLabel = "未知"
//      var twoLabel = "未知"
//      var threeLabel = "未知"
//      try {
//        oneLabel = tagFiles(0)
//        twoLabel = tagFiles(1)
//        threeLabel = tagFiles(2)
//      } catch {
//        case e: Exception => {
//          e.printStackTrace()
//        }
//      }
//
//      //      //Redis key-cache-liveness-domain-label域名匹配不上 从broadband-probe-cache-dns-authDomain拿权威域名匹配
//      //      if (oneLabel.equals("未知") && threeLabel.equals("未知") && twoLabel.equals("未知")) {
//      //        //域名转换为权威域名
//      //        val authDomain = Utils.domian2Authority(tuple._2)
//      //        threeLabel = broadcastBaseTags.value.getOrElse(authDomain, threeLabel)
//      //      }
//      (tuple._1, (oneLabel, twoLabel, threeLabel))
//    }).filter(line => line._2._1 != "未知" && line._2._2 != "未知" && line._2._3 != "未知")
//      .groupByKey()
//      .map(itr => {
//        var buff = new ArrayBuffer[ArrayBuffer[String]]()
//        var tagsBuff = new ArrayBuffer[String]()
//        val tagList = itr._2.toList
//        //把标签装进数组
//        for (index <- 0 to tagList.size - 1) {
//          tagsBuff += tagList(index)._1
//          tagsBuff += tagList(index)._2
//          tagsBuff += tagList(index)._3
//        }
//        buff += tagsBuff.distinct
////        UserDomainTags(itr._1, buff)
//      })
//
//    EsSpark.saveToEs(userAndDomainTags, s"bigdata_user_tags_${yesterday.substring(0, 8)}/tags")

    //关闭资源
    sc.stop()
  }
}
