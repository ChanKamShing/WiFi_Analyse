package com.cjs

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.control.Breaks

/**
  * 每天深访率：进入店铺深度访问的顾客占比（占总体客流）
  * 平均访问时间
  * 新、老顾客数
  * 访客总数等指标
  */
object base_day_analyse {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .set("spark.some.config.some","some-value")

        val ss = SparkSession
            .builder()
            .config(conf)
            .appName("base day analyse")
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

        var lastCustomerNum = 0
        var nowCustomerNum = 0  //当前总客户
        var newCustomerNum = 0  //新客户
        var oldCustomerNum = 0  //老客户
        var intervalCustomerNum = 0 //一天的访客量

        while (nowTime<=maxTime) {
            outer.breakable{    //异常捕捉
                var jumpNum = 0     //3分钟内，离开店铺的用户数量
                var visitNum = 0    //当天访问的数量
                var deepInNum = 0   //逗留时间超过半个小时的用户数量，深访数量
                var avgStayTime = 0  //当天内，平均每次访问逗留的时间

                var time1 = nowTime     //起始时间
                var time2 = nowTime+86400   //起始时间开始后的一天时间，86400 = 24*60*60

                //一天的访客量的sql
                var sqlTmp =
                    s"""
                      |select count(distinct mac) as num from visit
                      | where 'in_time' between ${time1} and ${time2}
                      | and stay_time > 0
                    """.stripMargin
                intervalCustomerNum = (ss.sql(sqlTmp).collect())(0)(0).toString.toInt

                //一开始到当前时间的一天后的访客量
                sqlTmp =
                    s"""
                      |select count(distinct mac) as num from visit
                      | where 'in_time' between ${minTime} and ${time2}
                      | and stay_time > 0
                    """.stripMargin
                nowCustomerNum = (ss.sql(sqlTmp).collect())(0)(0).toString.toInt

                //sql用了distinct，所以intervalCustomerNum >= newCustomerNum
                newCustomerNum = nowCustomerNum - lastCustomerNum
                oldCustomerNum = intervalCustomerNum - newCustomerNum

                //当天时间里面，3分钟（180秒）内离开店铺的用户的数量,跳出数量
                sqlTmp =
                    s"""
                      |select count(*) as jump_num from visit
                      | where 'in_time' between ${time1} and ${time2}
                      | and stay_time <= 180
                    """.stripMargin
                jumpNum = (ss.sql(sqlTmp).collect())(0)(0).toString.toInt

                //当天时间里面，超过半个小时（1200秒）才离开店铺的用户的数量，深访数量
                sqlTmp =
                    s"""
                       |select count(*) as deep_in_num from visit
                       | where 'in_time' between ${time1} and ${time2}
                       | and stay_time >= 1200
                    """.stripMargin
                deepInNum = (ss.sql(sqlTmp).collect())(0)(0).toString.toInt

                sqlTmp =
                    s"""
                       |select count(*) as visit_num, avg(stay_time) as avg_stay_time from visit
                       | where 'in_time' between ${time1} and ${time2}
                    """.stripMargin
                val row = (ss.sql(sqlTmp).collect())(0).asInstanceOf[Row]
                visitNum = row.getInt(0)
                avgStayTime = row.getInt(1)

                //跳出率
                var jumpRate = jumpNum.toFloat / visitNum.toFloat
                //深访率
                var deepInRate = deepInNum.toFloat / visitNum.toFloat
                //标准化格式
                var formatJumpRate = f"${jumpRate%1.2f}"
                var formatDeepInRate = f"${deepInRate%1.2f}"

                var dayString =
                    s"""{"time":${time1},"jump_out_rate":${formatJumpRate},"deep_in_rate":${formatDeepInRate},"avg_stay_time":${avgStayTime},"new_num":${newCustomerNum},"old_num":${oldCustomerNum},"customer_num":${visitNum}}\n""".stripMargin

                resultStr += dayString

                nowTime = time2
                lastCustomerNum = nowCustomerNum
            }
        }

        //将结果存进文件
        import  java.io._
        val targetPath = "/spark_data/base_day_analyse.json"
        val writer = new PrintWriter(new File(targetPath))
        writer.write(resultStr)

        writer.close()
        ss.sql("uncache table visit")
    }
}
