import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._

import scala.util.control.Breaks._

object Analysis {
  val spark: SparkSession = SparkSession.builder()
    .appName("Xiamen Job Analysis")
    .master("local")
    .getOrCreate()

  /**
   * 将分析结果保存到 csv 文件中
   * @param dataframe 原始数据集
   * @param path 保存 csv 文件名称
   */
  def writeResultIntoCSV(dataframe: DataFrame, path: String): Unit = {
    val basePath = "/home/hadoop/Desktop/analysis/"
    dataframe.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(basePath + path)
  }

  def main(args: Array[String]): Unit = {
    // csv 数据集路径
    val datasetPath = "hdfs://localhost:9000/input/dataset.csv"
    // 设置日志等级
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    // 使用 spark-csv 读取 csv 文件
    val dataframe = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(datasetPath)

    //  根据职业类型统计平均薪水
    // statAverageSalaryByJobCategory(dataframe)

    // 统计不同行业领域岗位需求
    // statNumberByJobCategory(dataframe)

    // 统计关键词热度
    // statKeywordPopularity(dataframe)

    // 统计经验需求与学历需求和薪资、公司规模的关系
    // statExperienceAndEducationRequirement(dataframe)

    // 统计各公司招聘岗位数量
    // statJobNumberByCompany(dataframe)

    // 统计不同地区招聘岗位数量
    // statJobArea(dataframe)
  }

  /**
   * 根据职业类型统计平均薪水
   * @param dataframe 原始数据集
   */
  def statAverageSalaryByJobCategory(dataframe: DataFrame): Unit = {
    // 定义 UDF (User defined functions)，根据月薪范围和年终奖情况，计算平均年薪
    val calcYearSalary = udf((min_salary: Int, max_salary: Int, annual: String) => {
      val monthAverage = (min_salary.toDouble + max_salary.toDouble) / 2.0
      val months = if (annual == null) 12 else annual.substring(0, annual.length - 1).toDouble
      months * monthAverage / 10
    })
    // 注册 UDF
    spark.udf.register("calcYearSalary", calcYearSalary)

    // 使用 spark-sql，从 DataFrame 中筛选数据
    val salaryAverageDataframe = dataframe
      .select("job_category", "min_salary", "max_salary", "annual_bonus")
      .withColumn("year_salary_average", calcYearSalary(
        col("min_salary"),
        col("max_salary"),
        col("annual_bonus")))                           // 作用 UDF，添加列 `year_salary_average`
      .groupBy("job_category")                             // 根据 `job_category` 分类
      .agg(
        bround(avg("min_salary"), 1).alias("min_salary_average"),
        bround(avg("max_salary"), 1).alias("max_salary_average"),
        bround(avg("year_salary_average"), 1).alias("year_salary_average")
      )
      .orderBy(asc("year_salary_average"))            // 根据平均年薪，从低到高排序

    salaryAverageDataframe.show()                                         // 展示数据
    writeResultIntoCSV(salaryAverageDataframe, "salary_average")    // 保存结果到 csv
  }

  /**
   * 统计不同行业领域的岗位需求
   * @param dataframe 原始数据集
   */
  def statNumberByJobCategory(dataframe: DataFrame): Unit = {
    val count = dataframe
      .select("job_category")
      .groupBy("job_category")
      .count()
      .orderBy(desc("count"))

    count.show()
    writeResultIntoCSV(count, "job_number_count_by_category")
  }


  /**
   * 使用 Map & Reduce 统计关键词热度
   * @param dataframe 原始数据集
   */
  def statKeywordPopularity(dataframe: DataFrame): Unit = {
    val keywords = dataframe
      .select("keywords")
      .rdd                                                 // 将 DataFrame 转为 RDD，方便执行 MapReduce
      .flatMap(line => line.getString(0).split("#"))       // 每列可能返回多个关键词，使用 flatMap 处理
      .filter(word => word.nonEmpty)                       // 过滤出非空的关键词
      .map(word => (word, 1))
      .reduceByKey((a, b) => (a + b))
      .sortBy(_._2, ascending = false)                     // 根据 value（统计个数）降序排列

    val keywordsFrame = spark.createDataFrame(keywords)    // RDD 转换成 DataFrame 并保存
    keywordsFrame.show()
    writeResultIntoCSV(keywordsFrame, "keyword_popularity")
  }

  /**
   * 统计经验需求与学历需求和薪资、公司规模的关系
   * @param dataframe 原始数据集
   */
  def statExperienceAndEducationRequirement(dataframe: DataFrame): Unit = {
    val experience = dataframe
      .select("experience_requirement", "job_category", "min_salary", "max_salary")
      .withColumn("salary_average", (col("min_salary") + col("max_salary")) / 2.0)
      .groupBy("experience_requirement")
      .agg(
        bround(avg("salary_average"), 1).alias("salary_average"),
        count("salary_average").alias("count")
      )
      .filter("count > 10")
      .orderBy(asc("salary_average"))

    experience.show()
    writeResultIntoCSV(experience, "experience_salary")

    val education = dataframe
      .select("education_requirement", "job_category", "min_salary", "max_salary")
      .withColumn("salary_average", (col("min_salary") + col("max_salary")) / 2.0)
      .groupBy("education_requirement")
      .agg(
        bround(avg("salary_average"), 1).alias("salary_average"),
        count("salary_average").alias("count")
      )
      .filter("count > 10")
      .orderBy(asc("salary_average"))
    education.show()
    writeResultIntoCSV(education, "education_salary")

    val experienceWithScale = dataframe
     .select("experience_requirement", "company_scale")
     .groupBy("experience_requirement", "company_scale")
     .count()
     .orderBy("experience_requirement", "company_scale")

    experienceWithScale.show()
    writeResultIntoCSV(experienceWithScale, "experience_with_scale")

    val educationWithScale = dataframe
      .select("education_requirement", "company_scale")
      .groupBy("education_requirement", "company_scale")
      .count()
      .orderBy("education_requirement", "company_scale")

    educationWithScale.show()
    writeResultIntoCSV(educationWithScale, "education_with_scale")
  }


  def statJobNumberByCompany(dataframe: DataFrame): Unit = {
    val numberCount = dataframe.select("company_name", "company_scale", "company_category", "job_name")
      .groupBy("company_name", "company_scale", "company_category")
      .agg(count("job_name").alias("count"))
      .orderBy(desc("count"))

    numberCount.show()
    writeResultIntoCSV(numberCount, "job_number_count")
  }

  def statJobArea(dataframe: DataFrame): Unit = {
    val area = dataframe.select("job_area")
      .rdd
      .flatMap(line => {
        val strItems = line.getString(0).split("·")
        if (strItems.length >= 3) Array(strItems(1) + "·" + strItems(2)) else Array("")
      })
      .filter(word => word.nonEmpty)
      .map(area => (area, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2, ascending = false)

    val areaFrame = spark.createDataFrame(area)
    areaFrame.show()
    writeResultIntoCSV(areaFrame, "job_area_detail")
  }

}