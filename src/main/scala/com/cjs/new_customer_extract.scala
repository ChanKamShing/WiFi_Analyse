package com.cjs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * 数据清洗
  * 原始hdfs的数据形式：{"tanzhen_id":"00aabbce","mac":"a4:56:02:61:7f:1a","time":"1492913100","rssi":"95","range":"1"}
  * 转换成新形式：{"mac":"a4:56:02:61:7f:1a","in_time":"xxxxxx","out_time":"xxxxxx","stay_time":"xxxxxx"}
  */
object new_customer_extract {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .set("spark.some.config.some","some-value")

        val ss = SparkSession
            .builder()
            .config(conf)
            .appName("custome_extract")
            .getOrCreate()

        import java.io._
        val writer = new PrintWriter(new File("/spark_data/visit_records.json"))

        //读取源文件
        val hdfs_path = "hdfs://master:9000/log1/log.log"
        val df = ss.read.json(hdfs_path)

        df.createOrReplaceTempView("data")

        ss.sql("cache table data")

        //获取所有用户的MAC,得到的是一个Array[Row]对象
        val macArray = ss.sql("select distinct mac from data").collect()
        //遍历每一个MAC
        for (mac <- macArray) {
            //mac是一个Row对象
            var resultString = ""
            var sql = "select 'time' from data where mac = '" + mac(0) + "'order by 'time'"

            val timeArray = ss.sql(sql).collect()

            //将timeArray转换成List
            var timeList = new ListBuffer[Int]
            for (time <- timeArray) {
                timeList += time(0).toString.toInt
            }

            var oldTime = 0
            var newTime = 0
            var startTime = 0
            var leaveTime = 0
            //最大时间间隔，表示若相邻两次时间超过这一时间，则认为这两个时间构成一次访问
            val maxVistTimeInterval = 300

            var k=0
            //遍历当前mac用户的time集合
            while(k < timeArray.length) {
                if (k == 0) {
                    //第一次遍历
                    oldTime = timeList(0)
                    newTime = timeList(0)
                    startTime = timeList(0)
                }else if (k == timeArray.length-1) {
                    //最后一次遍历
                    leaveTime = timeList(k)
                    var stayTime = leaveTime - startTime
                    resultString +=
                        s""""{"mac":${mac},"in_time":${startTime},"out_time":${leaveTime},"stay_time"${stayTime}}\n""".stripMargin
                }else{
                    newTime = timeList(k)

                    if ((newTime-oldTime)>maxVistTimeInterval) {
                        //相邻两次访问间隔大于分割阈值，则认为可以划分一次访问
                        leaveTime = newTime
                        var stayTime = leaveTime - startTime
                        resultString =
                            s"""{"mac":${mac},"in_time":${startTime},"out_time":${leaveTime},"stay_time":${stayTime}}\n""".stripMargin

                        startTime = newTime
                    }
                    oldTime = newTime
                }
                k+=1
            }
            //将结果集写入文件
            writer.write(resultString)
        }
        //关闭文件
        writer.close()
        ss.sql("uncache table data")
    }

}
