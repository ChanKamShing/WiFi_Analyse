package com.cjs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

/**
  * 离店数:每5分钟离开店铺的用户数量
  */
object leave_analyse {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .set("spark.some.config.some","some-value")

        val ss = SparkSession
            .builder()
            .config(conf)
            .appName("leave_analyse")
            .getOrCreate()

        var path = "/spark_data/visit_records.json" //经过数据清洗后的数据文件,字段包括{mac:标识不同的用户,in_time:用户进店时间,out_time:用户离开店的时间,stay_time:用户停留时间}
        val vistiRDF = ss.read.json(path)
        vistiRDF.createOrReplaceTempView("visit")
        ss.sql("cache table visit")

        import java.io._
        val writePath = "/spark_data/leave_num.json"
        val writer = new PrintWriter(new File(writePath))

        val sql = "select out_time from visit order by 'in_time'"

        val timeArr = ss.sql(sql).collect()
        //初始化时间
        val minTime = timeArr(0)(0).toString.toInt
        val maxTime = timeArr(timeArr.length-1)(0).toString.toInt
        var nowTime = minTime

        var outer = new Breaks

        while (nowTime<=maxTime) {
            outer.breakable{
                var leave_num = 0
                var time1 = nowTime
                var time2 = nowTime + 300

                var sqlTmp =
                    s"""
                       |select count(*) as num from visit
                       | where 'out_time' between ${time1} and ${time2}
                    """.stripMargin

                leave_num = (ss.sql(sqlTmp).collect())(0)(0).toString().toInt

                if (leave_num==0) {
                    nowTime += 300
                    outer.break()
                }

                var leaveStr =s"""{"time":${time1},"num":${leave_num}}\n""".stripMargin

                writer.write(leaveStr)  //尝试了与其他文件不同的写入逻辑,在新一次文件打开之前，该Writer对象是追加的方式写入

                nowTime += 300
            }
        }
        writer.close()
        ss.sql("uncache table visit")
    }

}
