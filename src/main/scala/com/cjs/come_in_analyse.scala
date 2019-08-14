package com.cjs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

/**
  * 入店率：comeNum/peopleFlowNum
  */
object come_in_analyse {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .set("spark.some.config.some","some-value")

        val ss = SparkSession
            .builder()
            .config(conf)
            .appName("come_in_analyse")
            .getOrCreate()

        var path = "/spark_data/visit_records.json" //经过数据清洗后的数据文件,字段包括{mac:标识不同的用户,in_time:用户进店时间,out_time:用户离开店的时间,stay_time:用户停留时间}
        val vistiRDF = ss.read.json(path)
        vistiRDF.createOrReplaceTempView("visit")
        ss.sql("cache table visit")

        path = "/spark_data/people_flow.json"
        val peopleFlowDF = ss.read.json(path)
        peopleFlowDF.createOrReplaceTempView("people_flow")
        ss.sql("cache table people_flow")

        var resultStr = ""

        var sql = "select in_time frome visit order by 'in_time'"
        val timeArr = ss.sql(sql).collect()

        //初始化时间
        val minTime = timeArr(0)(0).toString.toInt
        val maxTime = timeArr(timeArr.length-1)(0).toString.toInt
        var nowTime = minTime

        var outer = new Breaks

        while (nowTime<=maxTime) {
            outer.breakable {
                var comeNum = 0
                var time1 = nowTime
                var time2 = nowTime + 60

                var sqlTmp =
                    s"""
                       |select count(*) as num from visit
                       | where 'in_time' between ${time1} and ${time2}
                       | and stay_time > 0
                    """.stripMargin
                comeNum = (ss.sql(sqlTmp).collect()) (0)(0).toString.toInt

                if (comeNum == 0) {
                    nowTime += 60
                    outer.break() //结束本次outer
                }

                sqlTmp =
                    s"""
                       |select num from people_flow
                       | where 'in_time' = ${time1}
                    """.stripMargin
                var peopleFlowNum = (ss.sql(sqlTmp).collect())(0)(0).toString.toInt

                var inRate = comeNum.toFloat / peopleFlowNum.toFloat
                var formatInRate = f"${inRate%1.2f}"

                var inString = s"""{"time":${time1},"num":${comeNum},"in_rate":${formatInRate}}\n""".stripMargin

                resultStr += inString

                nowTime = time2
            }
        }

        //将结果存进文件
        import  java.io._
        val targetPath = "/spark_data/come_in_shop.json"
        val writer = new PrintWriter(new File(targetPath))
        writer.write(resultStr)

        writer.close()
        ss.sql("uncache table visit")
        ss.sql("uncache table people_flow")
    }

}
