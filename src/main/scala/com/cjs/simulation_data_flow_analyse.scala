package com.cjs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Random
import scala.util.control.Breaks

/**
  * 实时客流量，实际上是每60秒统计一次访客量
  */
object simulation_data_flow_analyse {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .set("spark.some.config.some","some-value")

        val ss = SparkSession
            .builder()
            .config(conf)
            .appName("simulation_data_flow_analyse")
            .getOrCreate()

        val path = "/spark_data/visit_records.json" //经过数据清洗后的数据文件,字段包括{mac:标识不同的用户,in_time:用户进店时间,out_time:用户离开店的时间,stay_time:用户停留时间}
        val vistiRDF = ss.read.json(path)
        vistiRDF.createOrReplaceTempView("visit")
        ss.sql("cache table visit")

        var resultStr = ""

        val sql = "select in_time from visit order by 'in_time'"
        val timeArr = ss.sql(sql).collect()

        //初始化时间
        val minTime = timeArr(0)(0).toString.toInt
        val maxTime = timeArr(timeArr.length-1)(0).toString.toInt
        var nowTime = minTime

        var outer = new Breaks

        while (nowTime<=maxTime) {
            outer.breakable{
                var comeNum = 0
                var time1 = nowTime
                var time2 = nowTime + 60

                var sqlTmp =
                    s"""
                       |select count(*) as num from visit
                       | where 'in_time' between ${time1} and ${time2}
                       | and stay_time > 0
                    """.stripMargin
                comeNum = (ss.sql(sqlTmp).collect())(0)(0).toString.toInt

                if (comeNum==0) {
                    nowTime += 60
                    outer.break()   //结束本次outer
                }

                var flowNum = comeNum
                var time = time1
                var rand = new Random

                var i = rand.nextInt(7)+4
                flowNum *= i

                var visitStr = s"""{"time":${time},"num":${flowNum}}\n""".stripMargin

                resultStr += visitStr

                nowTime = time2
            }
        }
        //将结果存进文件
        import  java.io._
        val targetPath = "/spark_data/people_flow.json"
        val writer = new PrintWriter(new File(targetPath))
        writer.write(resultStr)

        writer.close()
        ss.sql("uncache table visit")
    }

}
