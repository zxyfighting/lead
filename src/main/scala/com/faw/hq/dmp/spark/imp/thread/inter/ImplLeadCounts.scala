package com.faw.hq.dmp.spark.imp.thread.inter


import java.sql.DriverManager

import com.faw.hq.dmp.spark.imp.thread.contanst.KafkaParams
import com.faw.hq.dmp.spark.imp.thread.realize.LeadKafkaConsumer.{LeadCenterNew, LeadKey}
import com.faw.hq.dmp.spark.imp.thread.util.{JdbcUtil, JedisPoolUtils}
import com.mysql.jdbc.{Connection, PreparedStatement}
import org.apache.spark.streaming.dstream.DStream

/**
  * @ program: wxapp
  * @ description
  * @ author: ZhangXiuYun
  * @ create: 2020-01-13 17:26
  **/

object ImplLeadCounts {
  /**
    * 线索总量 计算
    *
    * */
  //总线索，是红旗店内线索量计算------根据处理后的数据，将结果累加
  def leadCounts(rdd: DStream[(String, Long)]): DStream[(String, Long)] = {
    rdd.updateStateByKey {
      case (seq, buffer) => { //seq序列当前周期中数量对集合，buffer表缓冲当中的值，所谓的checkPoint
        val sumCount = seq.sum + buffer.getOrElse(0L)
        Option(sumCount) //表往缓存里边更新对值　　它需要返回一个Option
      }
    }
  }

  //利用redis进行累加，将数据写入mysql中---计算是本店新线索量
  def newCounts(rdd:DStream[(LeadKey, Long)]): Unit ={
    rdd.foreachRDD(rdd=>{
      var conn: Connection = null
      var ps: PreparedStatement = null
      try {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        rdd.foreachPartition(rdd=>{
          var conn = DriverManager.getConnection(KafkaParams.url, KafkaParams.userName, KafkaParams.password);
          // 获取 redis 连接
          val jedisClient = JedisPoolUtils.getPool.getResource
          rdd.foreach(elem=>{
            var prekey = elem._1.lzDate+"_"+elem._1.vfrom1+"_"+elem._1.vfrom2+"_"+elem._1.vfrom3+"_"+elem._1.vfrom4 +"_"+elem._1.regionCode+"_"+elem._1.provinceCode+"_"+ KafkaParams.dateString
            jedisClient.expire(prekey,KafkaParams.timeout.toInt * 60 * 24)
            var preLeadCount:Long = jedisClient.hincrBy(prekey,"prelead",elem._2)
            var center = LeadCenterNew(KafkaParams.dateString, elem._1.vfrom1, elem._1.vfrom2, elem._1.vfrom3, elem._1.vfrom4, elem._1.regionCode,
              elem._1.provinceCode,preLeadCount)
            var count = JdbcUtil.findCount(conn,ps,elem._1.vfrom1,elem._1.vfrom2,elem._1.vfrom3,elem._1.vfrom4,elem._1.regionCode,elem._1.provinceCode)
            if(count>0)
            {
              JdbcUtil.updateChange(conn, ps,preLeadCount,elem._1.vfrom1,elem._1.vfrom2,elem._1.vfrom3,elem._1.vfrom4,elem._1.regionCode,elem._1.provinceCode)
            }
            else
            {
              JdbcUtil.insertChange(conn,ps,KafkaParams.dateString,center)
            }

          })
          jedisClient.close()
        })
      } catch {
        case t: Throwable => t.printStackTrace()
      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close();
        }
      }
    })

  }

}
