package com.faw.hq.dmp.spark.imp.thread.realize


import com.faw.hq.dmp.spark.imp.thread.contanst.KafkaParams
import com.faw.hq.dmp.spark.imp.thread.inter.{ImplJDBC, ImplLeadCounts, Implkafka}
import com.faw.hq.dmp.spark.imp.thread.util._
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream


/**
  *
  * auth:张秀云
  * purpose:线索中心指标：总线索量；是红旗内线索的统计；是本店内的新线索的统计
  */
object LeadKafkaConsumer {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    while (true) {
      //配置spark上下文环境对象
      val processInterval =59
      val conf = new SparkConf().setAppName("lead_center").setMaster("local[*]")
      val sc = new SparkContext(conf)
      //设置采集器每十秒批次拉取数据
      val sparkstream = new StreamingContext(sc, Seconds(processInterval))
      //利用checkpoint作为容错处理
      sparkstream.sparkContext.setCheckpointDir(KafkaParams.chkDir)

      // 初始化 Redis 连接池
      JedisPoolUtils.makePool(RedisConfig("prod.dbaas.private", 16359, 30000, "realtime123", 1000, 100, 50))
      val kafkaStream = KafkaRedisUtils.createDirectStream(sparkstream, KafkaParams.kafkaParams1, KafkaParams.module, KafkaParams.groupid, KafkaParams.topics)

      //将offset交给redis维护
      Implkafka.getOffset(KafkaParams.module,KafkaParams.groupid,kafkaStream)
      //将一条条json数据解
      val resultDestream1: DStream[String] = kafkaStream.map(_.value()).map(json => {
        val info: String = GetMethods.getLead(json)
        info
      })

      //resultSestream后面用到次数多，通过内存缓存，可以减少计算时间
      resultDestream1.cache()

      //将数据追加到hdfs上封装
      resultDestream1.foreachRDD(rdd => {
        Implkafka.toHDFS(rdd)
      })

      //获取连接oneid地址，将数据推送到oneId平台
      Implkafka.oneIdTo(resultDestream1)

      //求总线索量
      // 1.根据需求改变数据类型
      val leadsDest: DStream[(String, Long)] = resultDestream1.map(rdd => (KafkaParams.dateString, 1L))
      // 2.根据封装函数计算总线索量
      val leadIdCounts1: DStream[(String, Long)] = ImplLeadCounts.leadCounts(leadsDest)

      //将计算的线索量导入到mysql中
      val sql = "replace into dmp_behavior_clue_amounts(clue_dt,clue_amounts,create_time,update_time) values(?,?,?,now())"
      ImplJDBC.puvCounts(sql,leadIdCounts1)

      //求是红旗内线索的统计
      // 1.先简单过滤出是线索的数据
      val newLeadHQCounts1: DStream[(String, Long)] = resultDestream1.filter(rdd => {
        val strings = rdd.split(";")
        strings(18).equals("true")
      }).map(rdd => (KafkaParams.dateString, 1))

      // 2.根据过滤出的数据求得总的是红旗内线索量
      ImplLeadCounts.leadCounts(newLeadHQCounts1)

      //将数据写入mysql中
      val sql1 = "replace into dmp_behavior_clue_hq_amounts(clue_hq_dt,clue_hq_amounts,create_time,update_time) values(?,?,?,now())"
      ImplJDBC.puvCounts(sql1,newLeadHQCounts1)

      //是本店新线索的统计
      // 1.根据离线统计规则，按照（key，counts）统计
      val leadCenterDestream: DStream[(LeadKey,Long)] = resultDestream1.
        filter(rdd=>{
          val strings = rdd.split(";")
          !strings(19).equals("\\N") && !strings(14).equals("\\N") && !strings(15).equals("\\N") && !strings(16).equals("\\N")&& !strings(17).equals("\\N")&& !strings(10).equals("\\N") && !strings(8).equals("\\N") && strings(19).equals("true")
        }).
        map(rdd => {
        val strings = rdd.split(";")
        val leadCenter = LeadCenter(KafkaParams.dateString, strings(14), strings(15), strings(16), strings(17), strings(10), strings(8),strings(19).toBoolean, KafkaParams.dateString1, KafkaParams.dateString1)
        val key = LeadKey(KafkaParams.dateString, strings(14), strings(15), strings(16), strings(17), strings(10), strings(8))
        (key, 1L)
      })

     // 2.根据整理出的（key，value），进行累加
      val leadKey: DStream[(LeadKey, Long)] = leadCenterDestream.reduceByKey(_+_)
     // 3.将数据在redis累加并写入mysql中
      ImplLeadCounts.newCounts(leadKey)

     /*
      法二：问题：网络IO,磁盘IO，交互太过频繁，影响计算效率
     leadKey.foreachRDD(rdd=>{
        var conn: Connection = null
        var ps: PreparedStatement = null
        try {
          Class.forName("com.mysql.jdbc.Driver").newInstance();
        rdd.foreachPartition(rdd=>{
          var conn = DriverManager.getConnection(url, userName, password);
          // 获取 redis 连接
          val jedisClient = JedisPoolUtils.getPool.getResource
          rdd.foreach(rdd=>{
             rdd._2.foreach(elem=>{
               var keylead = "lead"+elem.lzDate+elem.vfrom1+elem.vfrom2+elem.vfrom3+elem.vfrom4 +elem.regionCode+elem.provinceCode +dateString
               var prekey = "prelead" +elem.lzDate+elem.vfrom1+elem.vfrom2+elem.vfrom3+elem.vfrom4 +elem.regionCode+elem.provinceCode+ dateString
               jedisClient.expire(keylead,timeout.toInt * 60 * 24)
               jedisClient.expire(prekey,timeout.toInt * 60 * 24)
               var leadCount: Long = jedisClient.hincrBy(keylead,"lead",1L)
               if (elem.newLeadDealer.==(true))
               {
                 var preLeadCount:Long = jedisClient.hincrBy(prekey,"prelead",1L)
               }
               else if(jedisClient.hexists(prekey,"prelead")){
                 var preLeadCount: Long = jedisClient.hget(prekey,"prelead").toLong
               }
               else {
                 var preLeadCount:Long = jedisClient.hset(prekey,"prelead","1")
               }
               var preLeadCount = jedisClient.hget(prekey,"prelead").toLong

               var center = LeadCenterNew(dateString, elem.vfrom1, elem.vfrom2, elem.vfrom3, elem.vfrom4, elem.regionCode,
                 elem.provinceCode,elem.newLeadDealer, leadCount,preLeadCount,dateString1, dateString1)
               var count = JdbcUtil.findCount(conn,ps,elem.vfrom1,elem.vfrom2,elem.vfrom3,elem.vfrom4,elem.regionCode,elem.provinceCode)
               if(count>0)
               {
                 JdbcUtil.updateChange(conn, ps,leadCount,preLeadCount,elem.vfrom1,elem.vfrom2,elem.vfrom3,elem.vfrom4,elem.regionCode,elem.provinceCode)
               }
               else
                 {
                   JdbcUtil.insertChange(conn,ps,dateString,center)
               }

             })

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
        })*/

      //求是本店内的新线索的统计
      val newLeadDealerCounts1: DStream[(String, Long)] = resultDestream1.filter(rdd => {
        val strings = rdd.split(";")
        strings(19).equals("true")
      }).map(rdd => (KafkaParams.dateString, 1))
      ImplLeadCounts.leadCounts(newLeadDealerCounts1)

      //将计算数据导入到mysql中
      val sql3 = "replace into dmp_behavior_clue_new_amounts(clue_new_dt,clue_new_amounts,create_time,update_time) values(?,?,?,now())"
      ImplJDBC.puvCounts(sql3,newLeadDealerCounts1)

      //开启采集器
      sparkstream.start()
      sparkstream.awaitTerminationOrTimeout(GetMethods.getSecondsNextEarlyMorning())
      sparkstream.stop(false, true)


    }
  }
  case class ThreadInfo(leadId: String, createTime: String, dealerId: String, dealerName: String, countyCode: String, countyName: String,
                          cityCode: String, cityName: String, provinceCode: String, provinceName: String, regionCode: String, regionName: String,
                          seriesCode: String, seriesName: String, vfrom1: String, vfrom2: String, vfrom3: String, vfrom4: String, newLeadHQ: Boolean, newLeadDealer: Boolean)

  case class LeadCenterNew(lzDate:String,vfrom1:String,vfrom2:String,vfrom3:String,vfrom4:String,regionCode:String,provinceCode:String,preLeadCount:Long)
  case class LeadCenter(lzDate:String,vfrom1:String,vfrom2:String,vfrom3:String,vfrom4:String,regionCode:String,provinceCode:String,newLeadDealer:Boolean,createTime:String,updateTime:String)
  case class LeadKey(lzDate:String,vfrom1:String,vfrom2:String,vfrom3:String,vfrom4:String,regionCode:String,provinceCode:String)
}