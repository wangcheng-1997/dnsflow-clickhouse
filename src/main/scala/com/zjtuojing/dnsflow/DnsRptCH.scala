package com.zjtuojing.dnsflow

import java.io.FileNotFoundException
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.concurrent.ThreadLocalRandom

import com.alibaba.fastjson.{JSON, JSONObject}
import com.tuojing.core.common.aes.AESUtils
import com.zjtuojing.dnsflow.BeanObj._
import com.zjtuojing.utils.{ClickUtils, DNSUtils}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis
import util.{IpSearch, IpUtil}

import scala.collection.mutable.ListBuffer
import scala.util.Random


/**
 * ClassName SparkStreamingReadHDFS
 * Date 2019/12/24 16:01
 **/
object DnsRptCH {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //文件分割时间间隔，秒
  val step = 10

  val random: ThreadLocalRandom = ThreadLocalRandom.current()

  /**
   * 基础数据聚合后写入es
   */
  def getDetailReport(detailRDD: RDD[EsDataBean], end: Long, now: String, spark: SQLContext): Unit = {
    val reportRdd = detailRDD.map(bean => {
      ((bean.clientName, bean.domain, bean.aip, bean.companyName, bean.authorityDomain, bean.soft, bean.websiteName, bean.websiteType), (bean.resolver, bean.inNet, bean.error))
    }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
      .filter(_._2._1 > 100)
      .map(line => {
        DnsReport(line._1._1, line._1._2, line._1._3, line._1._4, line._1._5, line._1._6, line._1._7, line._1._8, line._2._1, line._2._2, line._2._3, new Timestamp(end * 1000))
      })

    val report_df = spark.createDataFrame(reportRdd)
    ClickUtils.clickhouseWrite(report_df, "dns_flow_trend")
  }


  /**
   * 过滤包含Top20权威域名的所有数据
   */
  def Top20AuthorityDomain(baseRDD: RDD[DnsBean], businessIpRules: Array[(Long, Long, String, String, String)]): RDD[DnsBeanTop] = {
    val domainTop20 = baseRDD.filter(bean => {
      bean.domain.contains("qq.com") ||
        bean.domain.contains("baidu.com") ||
        bean.domain.contains("taobao.com") ||
        bean.domain.contains("p2cdn.com") ||
        bean.domain.contains("root-servers.net") ||
        bean.domain.contains("jd.com") ||
        bean.domain.contains("sina.com.cn") ||
        bean.domain.contains("microsoft.com") ||
        bean.domain.contains("360.com") ||
        bean.domain.contains("w3.org") ||
        bean.domain.contains("aliyun.com") ||
        bean.domain.contains("360.cn") ||
        bean.domain.contains("apple.com") ||
        bean.domain.contains("hicloud.com") ||
        bean.domain.contains("ys7.com") ||
        bean.domain.contains("163.com") ||
        bean.domain.contains("cuhk.edu.hk") ||
        bean.domain.contains("redhat.com") ||
        bean.domain.contains("miwifi.com") ||
        bean.domain.contains("vivo.com.cn")
    }).map(bean => {
      ((bean.clientName, bean.domain, bean.dnsIp, bean.aip), (bean.resolver, bean.error))
    }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .filter(_._2._1 > 100)
      .map(tuple => {
        val longIp = Utils.ipToLong(tuple._1._4)
        //资源名称 资源类型 资源属性
        var resourceName: String = null
        var resourceType: String = null
        var resourceProps: String = null
        businessIpRules.foreach(rule => {
          if (rule._1 <= longIp && rule._2 >= longIp) {
            resourceName = rule._3
            resourceType = rule._4
            resourceProps = rule._5
          }
        })
        DnsBeanTop(tuple._1._1, tuple._1._2, tuple._1._3, tuple._1._4, tuple._2._1, tuple._2._2, longIp, resourceName, resourceType, resourceProps)
      })
    domainTop20
  }

  /**
   * 把user domain resolver聚合后写入hdfs
   */
  def SaveUserInfo2HDFS(baseRDD: RDD[DnsBean], userMap: Map[String, String], now: String): Unit = {

    val reduced = baseRDD.mapPartitions(iter => new Iterator[((String, String), Int)]() {
      def hasNext: Boolean = {
        iter.hasNext
      }

      def next: ((String, String), Int) = {
        val bean = iter.next()
        ((bean.clientIp, bean.domain), 1)
      }
    })
      .reduceByKey(_ + _, 20)

    //      iter.hasNext
    //
    //      ((bean.clientIp, bean.domain), 1)
    //    }).reduceByKey(_ + _)

    logger.info("reduced.partitions.size: {}", reduced.partitions.size)

    reduced.map(tuple => {
      val userName = userMap.getOrElse(tuple._1._1, null)
      (userName, tuple._1._2, tuple._2)
    }).filter(tuple => StringUtils.isNotEmpty(tuple._1))
      .map(filtered => {
        filtered._1 + "\001" + filtered._2 + "\001" + filtered._3
      })
      .coalesce(12)
      .saveAsTextFile(s"hdfs://nns/dns_middle_data/${now.substring(0, 8)}/${now}")
  }


  /**
   * 获取权威Redis权威域名详细信息
   */
  def getAuthorityDomainMessage(jedis: Jedis): Array[authDomainMsg] = {
    val array = jedis.hgetAll("dns:dns-authDomain").values().toArray()
    //解析json
    var authDomain: String = null
    var websiteName: String = null
    var creditCode: String = null
    var companyType: String = null
    var companyName: String = null
    var companyAddr: String = null
    var onRecord: String = null
    var websiteType: String = null
    var soft: String = null
    var jobj = JSON.parseObject("")
    val authDomainTuple = array.map(arr => {
      try {
        jobj = JSON.parseObject(arr.toString)
        if (jobj.getString("authDomain") != "") {
          authDomain = jobj.getString("authDomain")
        }
        if (jobj.getString("websiteName") != "") {
          websiteName = jobj.getString("websiteName")
        }
        if (jobj.getString("creditCode") != "") {
          creditCode = jobj.getString("creditCode")

        }
        if (jobj.getString("companyType") != "") {
          companyType = jobj.getString("companyType")
        }
        if (jobj.getString("companyName") != "") {
          companyName = jobj.getString("companyName")
        }
        if (jobj.getString("companyAddr") != "") {
          companyAddr = jobj.getString("companyAddr")
        }
        if (jobj.getString("onRecord") != "") {
          onRecord = jobj.getString("onRecord")
        }
        if (jobj.getString("websiteType") != "") {
          websiteType = jobj.getString("websiteType")
        }
        if (jobj.getString("soft") != "") {
          soft = jobj.getString("soft")
        }

        authDomainMsg(authDomain, websiteName, creditCode, companyType, companyName, companyAddr, onRecord, websiteType, soft)
      } catch {
        case e: Exception => {
          logger.error("getAuthorityDomainMessage error", e)
          authDomainMsg(authDomain, websiteName, creditCode, companyType, companyName, companyAddr, onRecord, websiteType, soft)
        }
      }
    })
    authDomainTuple
  }


  /**
   * Top --clientName, clientIp, dnsIp, domain, aip
   * 聚合数据写入ES
   */
  def Tuple2Es(baseTopRdd: RDD[DnsBeanTop],
               inNetRule: Array[(Long, Long, String)],
               time: Long,
               businessIpRules: Array[(Long, Long, String, String, String)],
               authDomainBeans: Array[authDomainMsg]
              ): RDD[EsDataBean] = {
    //整理数据
    val resultRDD: RDD[EsDataBean] = baseTopRdd.map(bean => {
      //解析权威域名
      val authorityDomain = Utils.domian2Authority(bean.domain)
      var replaceDomain = bean.domain
      if (bean.domain.contains("http://")) replaceDomain = bean.domain.replace("http://", "")
      if (bean.domain.contains("https://")) replaceDomain = bean.domain.replace("https://", "")

      //内网
      var inNet = 0L
      //匹配大表
      inNetRule.foreach(tuple => {
        if (tuple._1 <= bean.longIp && tuple._2 >= bean.longIp) {
          inNet = bean.resolver
        } else {
          //匹配小表
          businessIpRules.foreach(rule => {
            if (rule._1 <= bean.longIp && rule._2 >= bean.longIp) {
              inNet = bean.resolver
            }
          })
        }
      })

      //权威域名详细数据
      var websiteName: String = null
      var creditCode: String = null
      var companyType = "未知"
      var companyName: String = null
      var companyAddr: String = null
      var onRecord: String = null
      var websiteType = "未知"
      var soft: String = null

      authDomainBeans.foreach(bean => {
        if (authorityDomain.equals(bean.authDomain)) {
          websiteName = bean.websiteName
          creditCode = bean.creditCode
          companyType = bean.companyType
          companyName = bean.companyName
          companyAddr = bean.companyAddr
          onRecord = bean.onRecord
          websiteType = bean.websiteType
          soft = bean.soft
        }
      })

      val maps = IpSearch.getRegionByIp(bean.aip)
      //电信 联通 国家 港澳台
      var abroadNum = 0L
      var telecomNum = 0L
      var linkNum = 0L
      var gatNum = 0L
      var aIpAddr: String = null
      if (!maps.isEmpty) {
        //获取国家，运营商
        val country = maps.get("国家").toString
        val operate = maps.get("运营").toString
        if (!country.equals("中国")) abroadNum = bean.resolver
        if (operate.contains("电信")) telecomNum = bean.resolver
        if (operate.contains("联通")) linkNum = bean.resolver
        //获取省份
        val province = maps.get("省份").toString
        if (province.contains("香港") || province.contains("澳门") || province.contains("台湾")) {
          gatNum = bean.resolver
        }
        //获取城市
        val city = maps.get("城市").toString
        var provinceAndCity = province ++ city
        if (city.equals(province)) provinceAndCity = province
        aIpAddr = country ++ provinceAndCity ++ operate
      }

      EsDataBean(bean.clientName, authorityDomain, bean.dnsIp, bean.aip, replaceDomain, bean.resolver, inNet, bean.error, new Timestamp(time * 1000),
        websiteName, creditCode, companyType, companyName, companyAddr, onRecord, websiteType, soft, bean.resourceName, bean.resourceType,
        bean.resourceProps, abroadNum, telecomNum, linkNum, gatNum, aIpAddr)
    })


    //        EsSpark.saveToEs(resultRDD, s"${bigDataDnsFlowClearPrefix}${now.substring(0, 8)}/dnsflow", Map("es.mapping.id" -> "key"))
    resultRDD
  }


  /**
   * 读取Redis key-cache-userinfo 获取电话号码 地址
   */
  def getPhoneNumAndAddress(jedis: Jedis): Map[String, String] = {
    var maps = Map[String, String]()
    try {
      val redisArray = jedis.hgetAll("broadband:userinfo").values().toArray()
      maps = redisArray.map(array => {
        val jobj = JSON.parseObject(array.toString)
        var mobile = "未知"
        var username = "未知"
        var doorDesc = "未知"
        if (jobj.getString("mobile") != "") {
          mobile = jobj.getString("mobile")
        }
        if (jobj.getString("username") != "") {
          username = jobj.getString("username")
        }
        if (jobj.getString("doorDesc") != "") {
          doorDesc = jobj.getString("doorDesc")
        }
        (username, doorDesc + "\t" + mobile)
      }).toMap
    } catch {
      case e: Exception =>
        maps = maps
        logger.error("getPhoneNumAndAddress", e)
      //        e.printStackTrace()
    }
    maps
  }


  /**
   * 读取Redis key-cache-online-user 获取用户名
   */
  def getUserName(jedis: Jedis): Map[String, String] = {
    var maps = Map[String, String]()
    try {
      maps = jedis.hgetAll("ONLINEUSERS:USER_OBJECT")
        .values().toArray
        .map(json => {
          val jobj = JSON.parseObject(json.toString)
//          val jobj = JSON.parseObject(AESUtils.decryptData("tj!@#123#@!tj&!$", json.toString))
          (jobj.getString("ip"), jobj.getString("user"))
        }).toMap
    } catch {
      case e: Exception =>
        maps = maps
        logger.error("getUserName", e)
      //        e.printStackTrace()
    }
    maps
  }

  /**
   * 获取Top用户相关信息
   */
  def getUserInfo(baseRDD: RDD[DnsBean], userMap: Map[String, String], phoneMap: Map[String, String], time: Long,
                  businessIpRules: Array[(Long, Long, String, String, String)],
                  inNetRule: Array[(Long, Long, String)], sc: SparkContext, spark: SQLContext): Unit = {
    //ES报表数据类型
    val types = "user"
    //暂时只计算客户名等于华数杭网的数据
    //    val last_key = s"dns:flow_top_dd:${df.format((time - 86400) * 1000).substring(0, 10)}"
    //    val redis_key = s"dns:flow_top_dd:${df.format(time * 1000).substring(0, 10)}"

    val dnsUserInfo = baseRDD
      //整理数据 计算内网数 错误数 获取用户名
      .map(bean => {
        ((bean.clientName, bean.clientIp, bean.domain, bean.aip), (bean.resolver, bean.error))
      }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .filter(_._1._1 == 1)
      .filter(_._2._1 > 50)
      .sortBy(_._2._1, ascending = false)
      .take(20000)

    val array = dnsUserInfo.map(top => {
      //客户名
      val userName = userMap.getOrElse(top._1._2, null)
      //内网
      var inNet = 0L
      val longIp = IpUtil.ipToLong(top._1._4)
      //匹配大表
      inNetRule.foreach(tuple => {
        if (tuple._1 <= longIp && tuple._2 >= longIp) {
          inNet = top._2._1
        } else {
          //匹配小表
          businessIpRules.foreach(rule => {
            if (rule._1 <= longIp && rule._2 >= longIp) {
              inNet = top._2._1
            }
          })
        }
      })
      ((top._1._1, userName, top._1._3, top._1._2), (top._2._1, top._2._2, inNet))
    }).filter(_._1._2 != null)

    val rdd1 = array.sortBy(-_._2._2)
      .take(2000)

    val userInfoes = rdd1.map(per => {
      val addressAndPhone = phoneMap.getOrElse(per._1._2, " " + "\t" + " ").split("\t")
      val accesstime = df.parse(df.format(time * 1000)).getTime / 1000
      //      types: String, clientName: Int, clientIp: String, userName: String, phone: String, address: String, domain: String,
      //      number: Long, accesstime: Long, error: Long, inNet: Long
      EsDnsUserInfo(types, per._1._1, per._1._4, per._1._2, addressAndPhone(1), addressAndPhone(0), per._1._3,
        per._2._1, new Timestamp(accesstime * 1000), per._2._2, per._2._3)
    })

    //    EsSpark.saveToEs(userInfoes, s"bigdata_dns_flow_top_user_${df.format(new Date()).substring(0, 4)}/aip")
    val userInfoes_df = spark.createDataFrame(userInfoes)
    ClickUtils.clickhouseWrite(userInfoes_df, "bigdata_dns_flow_top_user")

  }

  /**
   * Aip业务聚合
   */
  def getAipBusiness(baseTopRdd: RDD[DnsBeanTop], time: Long, spark: SQLContext): Unit = {
    //ES报表数据类型
    val types = "business"

    val businessRDD = baseTopRdd.map(bean =>
      (bean.clientName, bean.resourceName, bean.resourceType, bean.resourceProps, bean.resolver))

    //全量数据
    val allAipBusiness = businessRDD.map(tuple => ((tuple._2, tuple._3, tuple._4), tuple._5))
      .reduceByKey(_ + _)
      .map(reduced =>
        EsDnsAipBusiness(0, types, reduced._1._1, reduced._1._2, reduced._1._2 + "/" + reduced._1._3, reduced._2, new Timestamp(time * 1000)))
    //    EsSpark.saveToEs(allAipBusiness, dnsTop)
    val allAipBusiness_df = spark.createDataFrame(allAipBusiness)
    ClickUtils.clickhouseWrite(allAipBusiness_df, "dns_flow_top")

    //客户维度数据
    val clientAipBusiness = businessRDD.map(tuple => ((tuple._1, tuple._2, tuple._3, tuple._4), tuple._5))
      .reduceByKey(_ + _)
      .map(reduced =>
        EsDnsAipBusiness(reduced._1._1, types, reduced._1._2, reduced._1._3, reduced._1._3 + "/" + reduced._1._4, reduced._2, new Timestamp(time * 1000)))
    //    EsSpark.saveToEs(clientAipBusiness, dnsTop)
    val clientAipBusiness_df = spark.createDataFrame(clientAipBusiness)
    ClickUtils.clickhouseWrite(clientAipBusiness_df, "dns_flow_top")
  }

  /**
   * DNS服务器解析次数排名
   */
  def getDnsServerTopN(baseRDD: RDD[DnsBean], time: Long, spark: SQLContext): Unit = {
    //ES报表数据类型
    val types = "dnsIp"

    //全部数据
    val allDnsIpTopN = baseRDD.map(bean => (bean.dnsIp, 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsServerTop(0, types, reduced._1, reduced._2, new Timestamp(time * 1000)))
    //    EsSpark.saveToEs(allDnsIpTopN, dnsTop, Map("es.mapping.id" -> "key"))
    val allDnsIpTopN_df = spark.createDataFrame(allDnsIpTopN)
    ClickUtils.clickhouseWrite(allDnsIpTopN_df, "dns_flow_top")
    //客户维度数据
    val clientDnsIpTopN = baseRDD.map(bean => ((bean.clientName, bean.dnsIp), 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsServerTop(reduced._1._1, types, reduced._1._2, reduced._2, new Timestamp(time * 1000)))
    //    EsSpark.saveToEs(clientDnsIpTopN, dnsTop, Map("es.mapping.id" -> "key"))
    val clientDnsIpTopN_df = spark.createDataFrame(clientDnsIpTopN)
    ClickUtils.clickhouseWrite(clientDnsIpTopN_df, "dns_flow_top")

  }


  /**
   * Aip省份
   */
  def getProvinceNum(baseRDD: RDD[DnsBean], time: Long, spark: SQLContext): Unit = {
    //ES报表数据类型
    val types = "province"

    //全量数据
    val allAipProvince = baseRDD.map(bean => (bean.province, 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsAipProvince(0, types, reduced._1, reduced._2, new Timestamp(time * 1000)))

    val allAipProvince_df = spark.createDataFrame(allAipProvince)
    ClickUtils.clickhouseWrite(allAipProvince_df, "dns_flow_top")

    //客户维度数据
    val clientAipProvince = baseRDD.map(bean => ((bean.clientName, bean.province), 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsAipProvince(reduced._1._1, types, reduced._1._2, reduced._2, new Timestamp(time * 1000)))
    //    EsSpark.saveToEs(clientAipProvince, dnsTop)
    val clientAipProvince_df = spark.createDataFrame(clientAipProvince)
    ClickUtils.clickhouseWrite(clientAipProvince_df, "dns_flow_top")
  }


  /**
   * Aip运营商
   */
  def getOperatorTopN(baseRDD: RDD[DnsBean], time: Long, spark: SQLContext) = {
    //ES报表数据类型
    val types = "operator"

    //全量数据
    val allOperatorTopN = baseRDD.map(bean => (bean.operator, 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsAipOperator(0, types, reduced._1, reduced._2, new Timestamp(time * 1000)))
    //    EsSpark.saveToEs(allOperatorTopN, dnsTop)
    val allOperatorTopN_df = spark.createDataFrame(allOperatorTopN)
    ClickUtils.clickhouseWrite(allOperatorTopN_df, "dns_flow_top")

    //客户维度数据
    val clientOperatorTopN = baseRDD.map(bean => ((bean.clientName, bean.operator), 1L))
      .reduceByKey(_ + _)
      .map(reduced =>
        EsDnsAipOperator(reduced._1._1, types, reduced._1._2, reduced._2, new Timestamp(time * 1000)))
    //    EsSpark.saveToEs(clientOperatorTopN, dnsTop)
    val clientOperatorTopN_df = spark.createDataFrame(clientOperatorTopN)
    ClickUtils.clickhouseWrite(clientOperatorTopN_df, "dns_flow_top")
  }


  /**
   * 计算DNS每五分钟的QPS
   */
  def getDnsQps(baseRDD: RDD[DnsBean], time: Long, spark: SQLContext) = {
    val types = "qps"
    //全量数据
    val allQps = baseRDD.map(per => (types, (1L, per.error)))
      .reduceByKey((a, b) => {
        val num = a._1 + b._1
        val error = a._2 + b._2
        (num, error)
      })
      .map(rdd => {
        EsDnsQps(0, rdd._1, rdd._2._1, rdd._2._2, Math.floor(rdd._2._1 / 300).toLong, new Timestamp(time * 1000))
      })
    val allQps_df = spark.createDataFrame(allQps)

    ClickUtils.clickhouseWrite(allQps_df, "dns_flow_ratio")

    logger.info("全量qps")

    //客户维度数据
    val clientQps = baseRDD.map(bean => (bean.clientName, (1L, bean.error)))
      .reduceByKey((a, b) => {
        val num = a._1 + b._1
        val error = a._2 + b._2
        (num, error)
      })
      .map(rdd => {
        EsDnsQps(rdd._1, types, rdd._2._1, rdd._2._2, Math.floor(rdd._2._1 / 300).toLong, new Timestamp(time * 1000))
      })
    val clientQps_df = spark.createDataFrame(clientQps)
    ClickUtils.clickhouseWrite(clientQps_df, "dns_flow_ratio")
    logger.info("客户qps")
  }

  /**
   * 客户名 客户IP 域名 dns服务器 aip 五维度聚合
   */
  def dnsRpt(baseRDD: RDD[DnsBean], sc: SparkContext, businessIpRules: Array[(Long, Long, String, String, String)], domains: Array[String]): RDD[DnsBeanTop] = {

    val sorted = baseRDD.map(bean => {
      ((bean.clientName, bean.domain, bean.dnsIp, bean.aip), (bean.resolver, bean.error))
    }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(per => {
        val clientName = per._1._1
        val domain = per._1._2
        val dnsIp = per._1._3
        val aip = per._1._4
        var resolver = per._2._1
        val error = per._2._2
        if (domains.contains(domain)) {
          resolver += 10
        }
        ((clientName, domain, dnsIp, aip), (resolver, error))
      })
      //过滤条件
      .filter(_._2._1 >= 10)
      .sortBy(_._2._1, ascending = false)
      .take(70000)

    val rdd = sorted.map(tuple => {
      var resolver = tuple._2._1
      if (domains.contains(tuple._1._2)) {
        resolver -= 10
      }
      val longIp = Utils.ipToLong(tuple._1._4)
      //资源名称 资源类型 资源属性
      var resourceName: String = null
      var resourceType: String = null
      var resourceProps: String = null
      businessIpRules.foreach(rule => {
        if (rule._1 <= longIp && rule._2 >= longIp) {
          resourceName = rule._3
          resourceType = rule._4
          resourceProps = rule._5
        }
      })
      DnsBeanTop(tuple._1._1, tuple._1._2, tuple._1._3, tuple._1._4, resolver, tuple._2._2, longIp, resourceName, resourceType, resourceProps)
    })

    val resultRDD: RDD[DnsBeanTop] = sc.parallelize(rdd)
    resultRDD
  }


  /**
   * 计算响应代码占比
   */
  def getResponseCodeRatio(baseRDD: RDD[DnsBean], time: Long, spark: SQLContext) = {
    // ES报表数据类型
    val types = "responseCode"

    //全量数据响应代码占比
    val allResponseCodeNum = baseRDD.map(bean => (bean.responseCode, 1))
      .reduceByKey(_ + _)
      .map(line => EsResponseCode(0, line._1, line._2, types, new Timestamp(time * 1000)))
    val allResponseCodeNum_df = spark.createDataFrame(allResponseCodeNum)
    ClickUtils.clickhouseWrite(allResponseCodeNum_df, "dns_flow_ratio")

    //客户数据响应代码占比
    val clientResponseCodeNum = baseRDD.map(bean => ((bean.clientName, bean.responseCode), 1))
      .reduceByKey(_ + _)
      .map(line => EsResponseCode(line._1._1, line._1._2, line._2, types, new Timestamp(time * 1000)))
    val clientResponseCodeNum_df = spark.createDataFrame(clientResponseCodeNum)
    ClickUtils.clickhouseWrite(clientResponseCodeNum_df, "dns_flow_ratio")
  }

  /**
   * 各CODE top 域名
   *
   * @param baseRDD
   * @param time
   */
  def getResponseCodeDomainRatio(baseRDD: RDD[DnsBean], time: Long, spark: SQLContext): Unit = {
    // ES报表数据类型
    val types = "responseCodeDomain"
    val top = 5000
    // 全量数据响应代码占比

    // 域名维度
    val allResponseCodeNum: RDD[EsResponseCodeDomain] = baseRDD
      .map(bean => ((random.nextInt(100), bean.responseCode, bean.domain), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._2, bean._1._3), bean._2))
      .reduceByKey(_ + _)
      .map(bean => (bean._1._1, (bean._1._2, bean._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(top)
        topk.map(tk => EsResponseCodeDomain(0, tuple._1, tk._1, tk._2, types, new Timestamp(time * 1000)))
      })
    val allResponseCodeNum_df = spark.createDataFrame(allResponseCodeNum)
    //大表分月
    ClickUtils.clickhouseWrite(allResponseCodeNum_df, "dns_flow_response_ratio")

    // 泛域名维度
    val allResponseCodeNumAuthority = allResponseCodeNum
      .map(bean => ((bean.responseCode, Utils.domian2Authority(bean.domain)), bean.number))
      .reduceByKey(_ + _)
      .map(bean => (bean._1._1, (bean._1._2, bean._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(top)
        topk.map(tk => EsResponseCodeAuthorityDomain(0, tuple._1, tk._1, tk._2, "responseCodeAuthorityDomain", new Timestamp(time * 1000)))
      })
    val allResponseCodeNumAuthority_df = spark.createDataFrame(allResponseCodeNumAuthority)
    //大表分月
    ClickUtils.clickhouseWrite(allResponseCodeNumAuthority_df, "dns_flow_response_ratio")

    // 客户数据响应代码占比

    // 域名维度
    val clientResponseCodeNum = baseRDD
      .map(bean => ((random.nextInt(100), bean.responseCode, bean.domain, bean.clientName), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._2, bean._1._3, bean._1._4), bean._2))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._1, bean._1._3), (bean._1._2, bean._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(top)
        topk.map(tk => EsResponseCodeDomain(tuple._1._2, tuple._1._1, tk._1, tk._2, types, new Timestamp(time * 1000)))
      })
    val clientResponseCodeNum_df = spark.createDataFrame(clientResponseCodeNum)
    ClickUtils.clickhouseWrite(clientResponseCodeNum_df, "dns_flow_response_ratio")

    // 泛域名维度
    val clientResponseCodeNumAuthority = clientResponseCodeNum
      .map(bean => ((bean.responseCode, Utils.domian2Authority(bean.domain), bean.clientName), bean.number))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._1, bean._1._3), (bean._1._2, bean._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(top)
        topk.map(tk => EsResponseCodeAuthorityDomain(tuple._1._2, tuple._1._1, tk._1, tk._2, "responseCodeAuthorityDomain", new Timestamp(time * 1000)))
      })
    val clientResponseCodeNumAuthority_df = spark.createDataFrame(clientResponseCodeNumAuthority)
    ClickUtils.clickhouseWrite(clientResponseCodeNumAuthority_df, "dns_flow_response_ratio")

  }

  /**
   * 各CODE下top 用户
   *
   * @param baseRDD
   * @param time
   */
  def getResponseCodeClientIPRatio(baseRDD: RDD[DnsBean], time: Long, spark: SQLContext): Unit = {
    // ES报表数据类型
    val types = "responseCodeClientIP"
    //全量数据响应代码占比
    val allResponseCodeNum = baseRDD
      .map(bean => ((random.nextInt(100), bean.responseCode, bean.clientIp), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._2, bean._1._3), bean._2))
      .reduceByKey(_ + _)
      .map(line => (line._1._1, (line._1._2, line._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(500)
        topk.map(tk => EsResponseCodeClientIP(0, tuple._1, tk._1, tk._2, types, new Timestamp(time * 1000)))
      })
    //    val month = new SimpleDateFormat("yyyyMM").format(new Date().getTime)
    //    val dnsRatioMonthIndex = s"bigdata_dns_flow_ratio_${month}/ratio"
    val allResponseCodeNum_df = spark.createDataFrame(allResponseCodeNum)
    ClickUtils.clickhouseWrite(allResponseCodeNum_df, "dns_flow_response_ratio")

    //客户数据响应代码占比
    val clientResponseCodeNum = baseRDD
      .map(bean => ((random.nextInt(100), bean.responseCode, bean.clientIp, bean.clientName), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._2, bean._1._3, bean._1._4), bean._2))
      .reduceByKey(_ + _)
      .map(line => ((line._1._1, line._1._3), (line._1._2, line._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(500)
        topk.map(tk => EsResponseCodeClientIP(tuple._1._2, tuple._1._1, tk._1, tk._2, types, new Timestamp(time * 1000)))
      })
    val clientResponseCodeNum_df = spark.createDataFrame(clientResponseCodeNum)
    ClickUtils.clickhouseWrite(clientResponseCodeNum_df, "dns_flow_response_ratio")

  }

  /**
   * 计算响应类型占比
   */
  def getResponseTypeRatio(baseRDD: RDD[DnsBean], time: Long, spark: SQLContext) = {
    //ES报表数据类型
    val types = "responseType"

    //全量数据请求类型占比
    val allResponesNum = baseRDD.map(bean => ((random.nextInt(100), bean.responseType), 1))
      .reduceByKey(_ + _)
      .map(bean => (bean._1._2, bean._2))
      .reduceByKey(_ + _)
      .map(line => EsResponseType(0, line._1, line._2, types, new Timestamp(time * 1000)))
    val allResponesNum_df = spark.createDataFrame(allResponesNum)
    ClickUtils.clickhouseWrite(allResponesNum_df, "dns_flow_ratio")

    //客户数据请求类型占比
    val clientResponseNum = baseRDD.map(bean => ((bean.clientName, bean.responseType, random.nextInt(100)), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._1, bean._1._2), bean._2))
      .reduceByKey(_ + _)
      .map(line => EsResponseType(line._1._1, line._1._2, line._2, types, new Timestamp(time * 1000)))
    val clientResponesNum_df = spark.createDataFrame(clientResponseNum)
    ClickUtils.clickhouseWrite(clientResponesNum_df, "dns_flow_ratio")
  }


  /**
   * 计算请求类型占比
   */
  def getRequestTypeRatio(baseRDD: RDD[DnsBean], time: Long, spark: SQLContext) = {
    //ES报表数据类型
    val types = "requestType"

    //全量数据请求类型占比
    val allRequestNum = baseRDD.map(bean => (bean.requestType, 1))
      .reduceByKey(_ + _)
      .map(line => EsRequestType(0, line._1, line._2, types, new Timestamp(time * 1000)))
    val allRequestNum_df = spark.createDataFrame(allRequestNum)
    ClickUtils.clickhouseWrite(allRequestNum_df, "dns_flow.dns_flow_ratio")

    //客户数据请求类型占比
    val clientRequestNum = baseRDD.map(bean => ((bean.clientName, bean.requestType), 1))
      .reduceByKey(_ + _)
      .map(line => BeanObj.EsRequestType(line._1._1, line._1._2, line._2, types, new Timestamp(time * 1000)))
    val clientRequestNum_df = spark.createDataFrame(clientRequestNum)
    ClickUtils.clickhouseWrite(clientRequestNum_df, "dns_flow.dns_flow_ratio")
  }

  /**
   * 读取Mysql业务分组 资源类型=1  资源属性=2 资源名称=3 IP匹配规则
   */
  def getBusinessIpRules(spark: SQLContext) = {
    //导入隐式转换
    import spark.implicits._
    val businessRules = Utils.ReadMysql(spark, "dns_ip_segment_detail")
      .map(row => {
        val minIp = row.getAs[Long]("min_long_ip")
        val maxIp = row.getAs[Long]("max_long_ip")
        val resourceName = row.getAs[Int]("s_id").toString
        val resourceType = row.getAs[Int]("company_id").toString
        val resourceProps = row.getAs[Int]("resource_id").toString
        (minIp, maxIp, resourceName, resourceType, resourceProps)
      }).rdd.collect()
    businessRules
  }

  /**
   * 读取Mysql 获取aip击中内网网段ip段
   */
  def getInNetIpRule(spark: SQLContext): Array[(Long, Long, String)] = {
    //导入隐式转换
    import spark.implicits._
    val ipRule = Utils.ReadMysql(spark, "dns_media")
      .map(row => {
        val min_long_ip = row.getAs[Long]("min_long_ip")
        val max_long_ip = row.getAs[Long]("max_long_ip")
        val media_type = row.getAs[String]("media_type")
        (min_long_ip, max_long_ip, media_type)
      }).rdd.collect()
    ipRule
  }

  /**
   * 读取MySQL客户名IP规则数据
   */
  def getClientName(spark: SQLContext): Array[(Long, Long, Int)] = {
    //导入隐式转换
    import spark.implicits._
    val ipRDD = Utils.ReadMysql(spark, "dns_client_detail")
      .map(row => {
        val min_long_ip = row.getAs[Long]("min_long_ip")
        val max_long_ip = row.getAs[Long]("max_long_ip")
        val media_type = row.getAs[Int]("client_type_id")
        (min_long_ip, max_long_ip, media_type)
      }).rdd.collect()
    ipRDD
  }

  /**
   * 读取域名白名单数据
   */
  def getDomainWhitelist(spark: SQLContext): Array[String] = {
    //导入隐式转换
    import spark.implicits._
    val domains = Utils.ReadMysql(spark, "dns_white_domain")
      .map(row => {
        val domain = row.getAs[String]("domain")
        domain
      }).rdd.collect()
    domains
  }


  /**
   * 整理基本数据
   */
  def getBaseRDD(rdd: RDD[String], random: Random, clientIpRule: Array[(Long, Long, Int)], appTime: Long): RDD[DnsBean] = {

    rdd.mapPartitions(itr => {
      var dnsBeanBuffer = new scala.collection.mutable.ListBuffer[DnsBean]()
      itr.foreach(str => {
        var jobj: JSONObject = null
        try {
          jobj = JSON.parseObject(str)
        } catch {
          case e: Exception => {
            logger.error("JSON.parseObject, " + str, e)
          }
        }
        if (jobj != null) {
          val domain = jobj.getString("Domain")
          val logTimestamp = jobj.getLong("Timestamp")
          val serverIP = jobj.getString("ServerIP")
          val QR = jobj.getBoolean("QR")
          if (StringUtils.isNotEmpty(domain) && !domain.contains("master01")

            && !domain.contains(".localdomain")
            && !domain.contains(" ")
            && (!domain.contains("DHCP") || !domain.substring(domain.length - 4, domain.length).equals("DHCP"))
            && !domain.contains("HOST")
            && !domain.contains("Relteak")
            && !domain.contains("getCached")
            && !domain.contains("BlinkAP")
            && logTimestamp >= appTime && logTimestamp < appTime + 1000 * 60 * 5 //时间过滤
//            && util.IpUtil.isInRange(serverIP, "218.108.248.192/26")
            && QR == true
          ) {


            try {
              val dnsBean = DnsBean()

              /**
               * 请求类型
               * A,AAAA,NS,CNAME,DNAME,MX,TXT
               */
              dnsBean.requestType = jobj.getString("Type")
              /**
               * Answers字段信息
               */
              val answers = jobj.getJSONArray("Answers")

              /**
               * 获取响应代码
               * 0 -> NOERROR  成功的响应，这个域名解析成功
               * 2 -> SERVFAIL 失败的响应，这个域名的权威服务器拒绝响应或者响应REFUSE
               * 3 -> NXDOMAIN 不存在的记录，这个具体的域名在权威服务器中并不存在
               * 5 -> REFUSE   拒接，这个请求源IP不在服务的范围内
               */
              dnsBean.responseCode = jobj.getInteger("ResponseCode")
              //错误数 responseCode非0都是解析错误  || !requestType.equals("A")
              if (dnsBean.responseCode != 0 || answers.isEmpty) {
                dnsBean.error = 1
              }
              //域名
              dnsBean.domain = domain
              //DNS服务器
              dnsBean.dnsIp = serverIP

              /**
               * 获取客户名
               */
              dnsBean.clientIp = jobj.getString("ClientIP")
              val clientLong = Utils.ipToLong(dnsBean.clientIp)
              dnsBean.clientName = clientIpRule.find(tp => tp._1 <= clientLong && tp._2 >= clientLong)
                .headOption.getOrElse((0, 0, 5))._3

              /**
               * 解析Aip responseType
               *
               * 如果请求类型为A 解析Answers Aip随机取 responseType取aip对应的Type
               * 如果请求类型不为A 不解析Answers responseType:other
               */
              if ("A".equalsIgnoreCase(dnsBean.requestType)) {
                //Answers可能是""
                if (answers.size() > 0) {
                  var aIpTp = new collection.mutable.ListBuffer[String]()
                  var responseTypeTp = new collection.mutable.ListBuffer[String]()
                  //遍历answers数组  数组里嵌套的是json
                  for (index <- 0 to answers.size() - 1) {
                    val jobj = answers.getJSONObject(index)
                    val answerData = jobj.getString("Type")
                    responseTypeTp += answerData
                    if (answerData.equals("A")) aIpTp += jobj.getString("Value")
                  }
                  if (aIpTp.length > 0) dnsBean.aip = aIpTp(random.nextInt(aIpTp.length))
                  if (responseTypeTp.length > 0) dnsBean.responseType = responseTypeTp.last
                }
              }

              /**
               * 运营商  省份
               */
              val maps = IpSearch.getRegionByIp(dnsBean.aip)
              if (!maps.isEmpty) {
                dnsBean.operator = maps.get("运营").toString
                dnsBean.province = maps.get("省份").toString
              }

              if ("0.0.0.0".equals(dnsBean.aip)) dnsBean.error = 1

              //日志时间
              dnsBean.logTimestamp = jobj.getLong("Timestamp")
              dnsBeanBuffer += dnsBean
            } catch {
              case e: Exception =>
                logger.error("getBaseRDD", e)
            }
            //  DnsBean(clientName, responseCode, requestType, dnsIp, aIp, domain, resolver, error, operator, province, responseType, clientIP, logTimestamp)
          }
        }
      })
      dnsBeanBuffer.iterator
    })
  }

  def call(spark: SQLContext, sparkSession: SparkSession, sc: SparkContext, timestamp: Long): Unit = {

    val paths = new ListBuffer[String]()
    var endTime = 0L
    //多取前一个文件， 部分记录在前一个文件中
    //目录数据样本 hdfs://30.250.60.7/dns_log/2019/12/25/1621_1577262060
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    for (w <- 10 * 60 to 5 * 60 by -step) {
      endTime = timestamp - w * 1000
      val path = s"/dns_log/${
        Utils.timestamp2Date("yyyy/MM/dd/HHmmss", endTime - step * 1000)
      }_${
        endTime / 1000 - step
      }"

      try {
        if (hdfs.exists(new Path(path))) {
          //                  if (w >= 560) {
          paths += path
        } else {
          logger.info(s"file not exists $path")
        }
      } catch {
        case e: Exception =>
          logger.error(s"FileException $path", e)
      }
    }

    if (paths.isEmpty) {
      logger.info("paths size is  0,  empty ..... return ")
      return
    }

    val dnsPaths = paths.mkString(",")
    val beginTime = endTime - 1000 * 60 * 5
    val now = Utils.timestamp2Date("yyyyMMddHHmm", beginTime)

    /**
     * es 时间精确到秒
     */
    endTime = endTime / 1000

    logger.info("读取数据...")
    val dns_log = sc.textFile(dnsPaths)

    var baseRDD: RDD[BeanObj.DnsBean] = null
    var baseTopRdd: RDD[BeanObj.DnsBeanTop] = null
    try {
      //创建Redis连接
      logger.info("init jedis...")
      val jedisPool = JedisPool.getJedisPool()
//      val jedisPoolUser = JedisPoolUser.getJedisPool()

      val jedis: Jedis = JedisPool.getJedisClient(jedisPool)
//      val jedisUser: Jedis = JedisPoolUser.getJedisClient(jedisPoolUser)

      //读取Mysql客户名ip规则数据
      logger.info("读取Mysql客户名ip规则数据...")
      val clientIpRule = getClientName(spark)

      //读取Mysql域名白名单数据
      logger.info("读取Mysql域名白名单数据...")
      val domains = getDomainWhitelist(spark)

      //读取aip击中内网ip规则数据
      logger.info("读取aip击中内网ip规则数据...")
      val inNetRule = getInNetIpRule(spark)
      //
      //读取aip业务网段
      logger.info("读取aip业务网段...")
      val businessIpRules = getBusinessIpRules(spark)

//      //读取Redis在线用户表
//      logger.info("读取Redis在线用户表...")
//      val userMap = getUserName(jedisUser)
//      val userMapValue: Broadcast[Map[String, String]] = sc.broadcast(userMap)
//
//      //读取Redis详细信息
//      logger.info("读取Redis详细信息...")
//      val phoneAndAddress = getPhoneNumAndAddress(jedisUser)
//      val phoneAndAddressValue: Broadcast[Map[String, String]] = sc.broadcast(phoneAndAddress)

      //读取权威域名详细信息
      logger.info("读取权威域名详细信息...")
      val authDomainMsgs = getAuthorityDomainMessage(jedis)
      val authDomainMsgsValue: Broadcast[Array[authDomainMsg]] = sc.broadcast(authDomainMsgs)

      //TODO 整理原始数据 返回RDD[DnsBean] 并把数据持久化到内存+磁盘
      baseRDD = getBaseRDD(dns_log, random, clientIpRule, beginTime)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      logger.info("dnsRDD.partitions.size: {} ", baseRDD.partitions.size);

      //      logger.info("SaveUserInfo2HDFS...")
      //      SaveUserInfo2HDFS(baseRDD, userMapValue.value, now)

      //TODO 1.0 qps
      logger.info("getDnsQps...")
      getDnsQps(baseRDD, endTime, spark)

      //TODO 1.1 请求类型占比
      logger.info("请求类型占比...")
      getRequestTypeRatio(baseRDD, endTime, spark)


      //TODO 1.2 响应类型占比
      logger.info("getResponseTypeRatio...")
      getResponseTypeRatio(baseRDD, endTime, spark)


      //TODO 1.3 响应代码占比
      logger.info("getResponseCodeRatio...")
      getResponseCodeRatio(baseRDD, endTime, spark)

      //      响应代码top域名
      logger.info("响应代码top域名...")
      getResponseCodeDomainRatio(baseRDD, endTime, spark)
      //响应代码top clientip
      logger.info("响应代码top clientip...")
      getResponseCodeClientIPRatio(baseRDD, endTime, spark)

      //TODO 1.4 计算维度统计数据
      logger.info("计算维度统计数据...")
      baseTopRdd = dnsRpt(baseRDD, sc, businessIpRules, domains)
      //持久化Top数据
      baseTopRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

      //TODO 1.4.0 Top数据写入ES
      logger.info("Top数据写入ES...")
      val detailReportRDD = Tuple2Es(baseTopRdd, inNetRule, endTime, businessIpRules, authDomainMsgsValue.value)

      //      //报表数据写入ES
      logger.info("报表数据写入ES...")
      getDetailReport(detailReportRDD, endTime, now, spark)


//      //TODO 1.4.1 计算用户相关信息
//      logger.info("计算用户相关信息...")
//      getUserInfo(baseRDD, userMapValue.value, phoneAndAddressValue.value, endTime, businessIpRules, inNetRule, sc, spark)

      //TODO 1.5 Aip运营商排名
      logger.info("Aip运营商排名...")
      getOperatorTopN(baseRDD, endTime, spark)

      //TODO 1.6 Aip省份计算
      logger.info("Aip省份计算...")
      getProvinceNum(baseRDD, endTime, spark)

      //TODO 1.7 DNS服务器解析次数排名
      logger.info("DNS服务器解析次数排名...")
      getDnsServerTopN(baseRDD, endTime, spark)

      //TODO 1.8 Aip业务
      logger.info("Aip业务...")
      getAipBusiness(baseTopRdd, endTime, spark)

      val result_df = spark.createDataFrame(detailReportRDD)
      //大表按天
      ClickUtils.clickhouseWrite(result_df, "dns_flow_clear")

      jedis.close()
//      jedisUser.close()
      jedisPool.destroy()
//      jedisPoolUser.destroy()

    } catch {
      case e: FileNotFoundException =>
        logger.error("FileNotFoundException", e)
      case e: IllegalArgumentException =>
        logger.error("IllegalArgumentException", e)
    } finally {
      //把持久化的数据释放掉
      if (baseRDD != null) {
        baseRDD.unpersist()
        logger.info("dnsRDD.unpersist...")
      }
      if (baseTopRdd != null) {
        baseTopRdd.unpersist()
        logger.info(" baseTopRdd.unpersist...")
      }
    }

    logger.info("-----------------------------[批处理结束]")
  }

  def main(args: Array[String]): Unit = {
    val properties = DNSUtils.loadConf()
    val conf = new SparkConf()
    conf.setAppName("SparkStreamingReadHDFS")
    //    conf.setMaster("local[*]")
    //采用kryo序列化库
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    conf.registerKryoClasses(
      Array(
        classOf[Array[String]],
        classOf[DnsBean],
        classOf[DnsBeanTop],
        classOf[EsDataBean],
        classOf[EsDnsUserInfo],
        classOf[EsDnsUserInfoDd],
        classOf[EsResponseType],
        classOf[EsRequestType],
        classOf[EsResponseCode],
        classOf[EsDnsQps],
        classOf[EsDnsAipOperator],
        classOf[EsDnsAipProvince],
        classOf[EsDnsServerTop],
        classOf[EsDnsAipBusiness],
        classOf[authDomainMsg],
        classOf[EsDataBean],
        classOf[EsDataBeanDd],
        classOf[EsDataBeanDF]
      )
    )
    val sparkContext = SparkContext.getOrCreate(conf)
    //    sparkContext.setLogLevel("debug")
    //HDFS HA
    sparkContext.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sparkContext.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sparkContext.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sparkContext.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    //    test(sparkContext)
    //创建SparkSQL实例
    val spark = new SQLContext(sparkContext)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    //    call(spark, sparkContext, 0)
    // 用textFileStream的API定时读取一个空的文件 实现实时框架调度离线程序
    // /dns_log/test这个文件一定要保留
    val ssc = new StreamingContext(sparkContext, Seconds(300))
    val stream = ssc.textFileStream("hdfs://nns/DontDelete")
    stream.foreachRDD((rdd, time) => {
      logger.info("task start ..... batch time:{}", time)
      //      获取当前时间
      call(spark, sparkSession, sparkContext, time.milliseconds)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
