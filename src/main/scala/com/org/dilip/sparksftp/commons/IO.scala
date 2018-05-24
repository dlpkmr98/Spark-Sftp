package com.org.dilip.sparksftp.commons

import org.apache.spark.sql.DataFrame
import com.org.dilip.spark.SparkSupport
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.functions._
import com.org.dilip.sparksftp.sparksftp.RorD




trait IO {
  
  def convert(x:Option[RorD]) = x

  def toReadDataFromParquet(implicit path: String = "src/main/resources/input/"): DataFrame = SparkSupport.sqlCtx.read.parquet(path)

  def toReadDataFromCsv(implicit path: String = "src/main/resources/input/"): DataFrame = SparkSupport.sqlCtx.read.format("com.databricks.spark.csv")
    .option("header", "true").option("inferSchema", "true").load(path)

  def toWriteParquet(df: DataFrame, fileName: String, path: String = "src/main/resources/input/", mode: String = "overwrite") =
    df.write.mode(mode).format("parquet").save(path + fileName)

  def writeDataToCsv(df: DataFrame, fileName: String, path: String = "src/main/resources/output/", sep: String = ",", mode: String = "overwrite") =
    df.coalesce(1).write.mode(mode).format("com.databricks.spark.csv").option("sep", sep).save(path + fileName)

  lazy val sc = SparkSupport.sc
  lazy val sqlCtx = SparkSupport.sqlCtx

  /**
   * Reads a parquet to dataframe
   * @param path path to read from
   * @return DataFrame read
   */
  def readToDataFrame(path: String): DataFrame = {
    sqlCtx.read.parquet(path)
  }

  /**
   * Reads a CSV file to RDD
   * @param file path to file to read
   * @param delim delimiter to split by
   * @return RDD of Row read
   */
  def readToRdd(file: String, delim: String = "\t"): RDD[Row] = {
    sqlCtx.read.format("com.databricks.spark.csv").option("delimiter", delim).load(file).rdd
  }

  /**
   * reads mutiple CSVs and parses them to DataFrame
   * @param path path to read from
   * @param delimiter delimiter to split by
   * @param filenameIncluded include filename as extra column
   * @param header specifies if file includes headers
   * @param inferSchema infers schema
   * @param nullValue value to replace nulls with
   * @param fileExtension file extension of file on disk
   * @param mode read mode to use (see com.databricks.spark.csv)
   * @param schema optional schema if not inferred
   * @param fileSizeIncluded adds additional column with file size
   * @return CSVs parsed into DataFrame
   */
  def readCSVsToDataFrame(
    path:             String,
    delimiter:        String             = ",",
    filenameIncluded: Boolean            = false,
    header:           Boolean            = true,
    inferSchema:      Boolean            = true,
    nullValue:        String             = "",
    fileExtension:    String             = ".csv",
    mode:             String             = "PERMISSIVE",
    schema:           Option[StructType] = None,
    fileSizeIncluded: Boolean            = false): DataFrame = {

    def getListOfFiles(path: String): List[FileStatus] = {
      val conf = sc.hadoopConfiguration

      val fs = FileSystem.get(conf)
      val filter = new PathFilter() {
        def accept(file: Path): Boolean = {
          file.getName().endsWith(fileExtension)
        }
      }
      fs.listStatus(new Path(path), filter).toList
    }

    val df: List[DataFrame] = getListOfFiles(path).map(f => {
      val fileSize = f.getLen
      val fileSplit = f.getPath.toString.split("/")
      val filename = fileSplit(fileSplit.length - 1)

      (filenameIncluded, fileSizeIncluded) match {
        case (true, false)  => readCSVToDataFrame(f.getPath.toString, delimiter, header, inferSchema, nullValue, mode, schema).withColumn("filename", lit(filename))
        case (true, true)   => readCSVToDataFrame(f.getPath.toString, delimiter, header, inferSchema, nullValue, mode, schema).withColumn("filename", lit(filename)).withColumn("filesize", lit(fileSize))
        case (false, false) => readCSVToDataFrame(f.getPath.toString, delimiter, header, inferSchema, nullValue, mode, schema)
        case (false, true)  => readCSVToDataFrame(f.getPath.toString, delimiter, header, inferSchema, nullValue, mode, schema).withColumn("filesize", lit(fileSize))
      }
    })

    if (df.size > 1) df.reduce(_ unionAll _)
    else if (df.size == 1) df(0)
    else sqlCtx.emptyDataFrame
  }

  /**
   * for a single CSV.
   *
   * @param path path to read from
   * @param delimiter delimiter to split by
   * @param header specifies if file includes headers
   * @param inferSchema infers schema
   * @param nullValue value to replace nulls with
   * @param mode read mode to use (see com.databricks.spark.csv)
   * @param schema optional schema if not inferred
   * @return CSV parsed into DataFrame
   */
  def readCSVToDataFrame(
    path:        String,
    delimiter:   String,
    header:      Boolean            = true,
    inferSchema: Boolean            = true,
    nullValue:   String             = "",
    mode:        String             = "PERMISSIVE",
    schema:      Option[StructType] = None) = {

    val optionsMap = Map("header" -> header.toString, "delimiter" -> delimiter, "inferSchema" -> inferSchema.toString, "treatEmptyValuesAsNulls" -> "true", "nullValue" -> nullValue, "mode" -> mode)
    val fnDfReader: DataFrameReader = sqlCtx.read.format("com.databricks.spark.csv").options(optionsMap)

    schema match {
      case None    => fnDfReader.load(path)
      case Some(m) => fnDfReader.schema(m).load(path)
    }
  }

  /**
   * Writes a dataframe into a partitioned parquet structure with table metadata
   * @param df dataframe to write
   * @param tableName table name
   * @param path path to write to
   * @param mode write mode (overwrite, append, ...)
   */
  def writePartitionedParquetTable(df: DataFrame, tableName: String, path: String, mode: String = "overwrite"): Unit = {
    writePartitionedDF(df)
      .mode(mode)
      .format("parquet")
      .options(Map("path" -> path))
      .saveAsTable(tableName)
  }

  /**
   * @param df
   * @param col
   * @param mode
   * @param path
   */
  def writePartitionedParquet(df: DataFrame, col: String, mode: String = "append", path: String) = {
    df.write.mode(mode).partitionBy(col).parquet(path)
  }

  /**
   * Write a DataFrame as parquet
   * @param df DataFrame to write
   * @param path path to write to
   * @param mode write mode (overwrite, append, ...)
   */
  def writeParquet(df: DataFrame, path: String, mode: String = "append"): Unit = {
    df.write
      .mode(mode)
      .parquet(path)
  }

  /**
   * Write DatafFame as date partitioned parquet structure
   * @param df dataframe to write
   * @return
   */
  @deprecated("use writePartitionedParquetTable() instead")
  def writePartitionedDF(df: DataFrame): DataFrameWriter = {
    df.write.partitionBy("year", "month", "day")
  }

  /**
   * reads json into a DataFrame
   * @param json json to distribute
   * @return DataFrame with json rows
   */
  def jsonToDf(json: String): DataFrame = {
    readJson(sc.parallelize(json :: Nil))
  }

  /**
   * Read RDD of String holding json string representations to DataFrame
   * @param rdd rdd to parse
   * @return DataFrame parsed from json
   */
  def readJson(rdd: RDD[String]): DataFrame = {
    sqlCtx.read.json(rdd)
  }

  /**
   * write DataFrame as CSV textfile
   * @param df DataFrame to write
   * @param path path to write to
   * @param delimeter delimiter to use for CSV
   */
  def writeTextFile(df: DataFrame, path: String, delimeter: String = "\t"): Unit = {
    df.map { row: Row => row.mkString(delimeter) }.saveAsTextFile(path)
  }

}
