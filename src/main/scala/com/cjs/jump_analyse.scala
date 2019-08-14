package com.cjs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

/**
  * 每5分钟统计一次
  *     跳出率：3分钟内，离开的客户占总客户数量的比率
  *     跳出数：进店后，3分钟内离开的客户数量
  */
object jump_analyse {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .set("spark.some.config.some","some-value")

        val ss = SparkSession
            .builder()
            .config(conf)
            .appName("jump_analyse")
            .getOrCreate()

        var path = "/spark_data/visit_records.json" //经过数据清洗后的数据文件,字段包括{mac:标识不同的用户,in_time:用户进店时间,out_time:用户离开店的时间,stay_time:用户停留时间}
        val vistiRDF = ss.read.json(path)
        vistiRDF.createOrReplaceTempView("visit")
        ss.sql("cache table visit")

        import java.io._
        val writePath = "/spark_data/jump_rate.json"
        val writer = new PrintWriter(new File(writePath))

        val sql = "select in_time from visit order by 'in_time'"

        val timeArr = ss.sql(sql).collect()
        //初始化时间
        val minTime = timeArr(0)(0).toString.toInt
        val maxTime = timeArr(timeArr.length-1)(0).toString.toInt
        var nowTime = minTime

        var outer = new Breaks

        while (nowTime<=maxTime) {
            outer.breakable{
                var time1 = nowTime
                var time2 = nowTime+300
                var jumpNum = 0
                var visitNum = 0

                var sqlTmp = s"select count(*) as num from visit where in_time between ${time1} and ${time2}"

                visitNum = (ss.sql(sqlTmp).collect())(0)(0).toString.toInt

                if (visitNum==0) {
                    nowTime += 300
                    outer.break()
                }

                sqlTmp = s"select count(*) as num from visit where in_time between ${time1} and ${time2} and 'stay_time'<=180"
                jumpNum = (ss.sql(sqlTmp).collect())(0)(0).toString.toInt

                val jumpRate = jumpNum.toFloat/visitNum.toFloat
                val formatJumpRate = f"${jumpRate%1.2f}"

                var jumpStr = s"""{"time":${time1},"jump_rate":${formatJumpRate},"jump_num":${jumpNum},"visit_num":${visitNum}}\n"""

                writer.write(jumpStr)

                nowTime += 300
            }
        }

        //关闭PrintWriter对象
        writer.close()
        //清空缓存表
        ss.sql("uncache table visit")
    }

}
