package com.zjtuojing.dnsflow

import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer

/**
 * ClassName BeanObj
 * Date 2019/11/22 15:30
 **/
object BeanObj {

  /**
   * DNS
   * 基本数据
   */
  case class DnsBean(var clientName: Int = 5, var responseCode: Int = 2, var requestType: String = "", var dnsIp: String = ""
                     , var aip: String = "0.0.0.0", var domain: String = "", var resolver: Long = 1, var error: Long = 0
                     , var operator: String = "", var province: String = "", var responseType: String = "other", var clientIp: String = ""
                     , var logTimestamp: Long = 0
                    )

  /**
   * DNS
   * 基本数据 Top5W
   */
  case class DnsBeanTop(clientName: Int, domain: String, dnsIp: String,
                        aip: String, resolver: Long, error: Long, longIp: Long,
                        resourceName: String, resourceType: String, resourceProps: String
                       )

  /**
   * 写入ES的5维度聚合
   * --clientName, clientIp, dnsIp, domain, aip
   *
   */
  case class EsDataBean(clientName: Int, authorityDomain: String, dnsIp: String, aip: String,
                        domain: String, resolver: Long, inNet: Long, error: Long, accesstime: Timestamp,
                        websiteName: String, creditCode: String, companyType: String,
                        companyName: String, companyAddr: String, onRecord: String, websiteType: String, soft: String,
                        resourceName: String, resourceType: String, resourceProps: String,
                        abroadNum: Long, telecomNum: Long, linkNum: Long, gatNum: Long, aIpAddr: String
                       )

  /**
   * 写入ES的2维度按天聚合
   * --clientName,domain
   *
   */
  case class EsTypeDataClearBean(key: String, clientName: Int, authorityDomain: String, aip: String, dnsIp: String,
                                 domain: String, var resolver: Long, var inNet: Long, var error: Long, accesstime: Long,
                                 var abroadNum: Long, var telecomNum: Long, var linkNum: Long, var gatNum: Long, updateTime: Long, oldKey: String, resourceTypes: List[Map[String, Any]])

  case class EsDataClearBean(key: String, clientName: Int, authorityDomain: String, aip: String, dnsIp: String,
                             domain: String, var resolver: Long, var inNet: Long, var error: Long, accesstime: Long,
                             var abroadNum: Long, var telecomNum: Long, var linkNum: Long, var gatNum: Long, updateTime: Long, oldKey: String)

  /**
   * 写入ES的5维度按天聚合
   * --clientName, clientIp, dnsIp, domain, aip
   *
   */
  case class EsDataBeanDd(key: String, clientName: Int, authorityDomain: String, dnsIp: String, aip: String,
                          domain: String, var resolver: Long, var inNet: Long, var error: Long, accesstime: Long,
                          websiteName: String, creditCode: String, companyType: String,
                          companyName: String, companyAddr: String, onRecord: String, websiteType: String, soft: String,
                          resourceName: String, resourceType: String, resourceProps: String,
                          var abroadNum: Long, var telecomNum: Long, var linkNum: Long, var gatNum: Long, aIpAddr: String, updateTime: Long
                         )

  case class EsDataBeanDF(key: String, clientName: Int, authorityDomain: String, dnsIp: String, aip: String, domain: String, var resolver: Long,
                          var inNet: Long, var error: Long, accesstime: String, websiteName: String, creditCode: String, companyType: String,
                          companyName: String, companyAddr: String, onRecord: String, websiteType: String, soft: String, resourceName: String,
                          resourceType: String, resourceProps: String, var abroadNum: Long, var telecomNum: Long, var linkNum: Long, var gatNum: Long, aIpAddr: String)


  /**
   * 用户名 地址 电话
   */
  case class EsDnsUserInfo(types: String, clientName: Int, clientIp: String, userName: String, phone: String, address: String, domain: String,
                           number: Long, accesstime: Timestamp, error: Long, inNet: Long)

  /**
   * 用户名 地址 电话 dd
   */
  case class EsDnsUserInfoDd(types: String, clientName: Int, clientIp: String, userName: String, phone: String, address: String, domain: String,
                             var number: Long, accesstime: Long, var error: Long, var inNet: Long, key: String)

  /**
   * 响应类型占比
   */
  case class EsResponseType(clientName: Int, responseType: String, number: Int, types: String, accesstime: Timestamp)


  /**
   * 请求类型占比R
   */
  case class EsRequestType(clientName: Int, requestType: String, number: Int, types: String, accesstime: Timestamp)


  /**
   * 响应代码占比
   */
  case class EsResponseCode(clientName: Int, responseCode: Int, number: Int, types: String, accesstime: Timestamp)

  case class EsResponseCodeDomain(clientName: Int, responseCode: Int, domain: String, number: Int, types: String, accesstime: Timestamp)

  case class EsResponseCodeClientIP(clientName: Int, responseCode: Int, clientIp: String, number: Int, types: String, accesstime: Timestamp)

  case class EsResponseCodeAuthorityDomain(clientName: Int, responseCode: Int, authorityDomain: String, number: Int, types: String, accesstime: Timestamp)


  /**
   * qps
   */
  case class EsDnsQps(clientName: Int, types: String, number: Long, error: Long, avgNum: Long, accesstime: Timestamp)


  /**
   * Aip运营商TopN
   */
  case class EsDnsAipOperator(clientName: Int, types: String, operator: String, number: Long, accesstime: Timestamp)


  /**
   * Aip省份
   */
  case class EsDnsAipProvince(clientName: Int, types: String, province: String, number: Long, accesstime: Timestamp)


  /**
   * DNS Server
   */
  case class EsDnsServerTop(clientName: Int, types: String, dnsIp: String, number: Long, accesstime: Timestamp)


  /**
   * Aip 业务聚合
   */
  case class EsDnsAipBusiness(clientName: Int, types: String, resourceName: String, resourceType: String, resourceProps: String, number: Long, accesstime: Timestamp)


  /**
   * 权威域名匹配数据
   */
  case class authDomainMsg(authDomain: String, websiteName: String, creditCode: String, companyType: String,
                           companyName: String, companyAddr: String, onRecord: String, websiteType: String, soft: String)


  /**
   * 用户域名解析次数Top10
   */
  case class UserDomainTop10(types: String, property: String, userName: String, domains: ArrayBuffer[Map[String, Any]], accesstime: Long)


  /**
   * 用户离线报表基础数据
   */
  case class UserRptBean(userName: String, domain: String, resolver: Long)


  /**
   * 用户全天总解析数
   */
  case class UserAllResolver(types: String, property: String, userName: String, resolver: Long, accesstime: Long)


  /**
   * 用户域名标签
   */
  case class UserDomainTags(userName: String, tags: ArrayBuffer[String])

  /**
   * flowmsg
   */
  case class FlowmsgData(timestamp: String, clientName: String, sourceIp: String, appName: String, bytes: Long)


  case class DnsReport(clientName: Int, domain: String, aip: String, companyName: String, authorityDomain: String, soft: String, websiteName: String, websiteType: String,
                       resolver: Long, inNet: Long, error: Long, accesstime: Timestamp
                      )

  /**
   * FlowTOP 省份维度
   */
  case class FTPBeanProvince(types: String, number: Long, province: String, clientName: Int, accesstime: Long)

  /**
   * FlowTOP 运营商维度
   */
  case class FTPBeanOperator(types: String, number: Long, operator: String, clientName: Int, accesstime: Long)

  /**
   * FlowTOP DnsIp维度
   */
  case class FTPBeanDnsIp(types: String, number: Long, dnsIp: String, clientName: Int, accesstime: Long)

  /**
   * FlowTOP 资源名称维度
   */
  case class FTPBeanBusiness(types: String, number: Long, clientName: Int, resourceProps: String, resourceName: String, resourceType: String, accesstime: Long)

  /**
   * FlowTOP 用户维度
   */
  case class FTPBeanUser(types: String, number: Long, inNet: Long, address: String, clientName: Int, phone: String, domain: String, clientIp: String,
                         error: Long, userName: String, accesstime: Long)

  /**
   * Ratio 请求类型维度
   */
  case class RatioBeanRequestType(types: String, number: Long, requestType: String, clientName: Int, accesstime: Long)

  /**
   * Ratio 响应代码维度
   */
  case class RatioBeanResponseCode(types: String, number: Long, responseCode: String, clientName: Int, accesstime: Long)

  /**
   * Ratio 响应类型维度
   */
  case class RatioBeanResponseType(types: String, number: Long, responseType: String, clientName: Int, accesstime: Long)

  /**
   * Ratio qps解析数维度
   */
  case class RatioBeanQps(types: String, number: Long, error: Long, avgNum: Long, clientName: Int, accesstime: Long)

  /**
   * Ratio 响应代码关联域名维度
   */
  case class RatioBeanResponseCodeDomain(types: String, number: Long, domain: String, clientName: Int, responseCode: String, accesstime: Long)

  /**
   * Ratio 响应代码关联源ip维度
   */
  case class RatioBeanResponseCodeClientIp(types: String, number: Long, clientIp: String, clientName: Int, responseCode: String, accesstime: Long)

  /**
   * Trend 所有字段维度
   */
  case class TrendBeanAll(domain: String, authorityDomain: String, clientName: Int, accesstime: Long, aip: String, inNet: Long, error: Long, resolver: Long,
                          websiteType: String, soft: String, websiteName: String, companyName: String)

}
