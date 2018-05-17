package com.org.dilip.sparksftp.jobs


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.SQLContext
import java.io.FileNotFoundException
import java.io.File
import com.org.dilip.sparksftp.UnitSpec
import com.org.dilip.sparksftp.commons.Properties

class SftpStreamSpec extends UnitSpec {

  implicit val fileName = Properties.file_name

  "SftpStream_init" should "return FileNotFoundException when pass illegal file path" in {
  an[FileNotFoundException] should be thrownBy new SftpStream().init(fileName)
  }
}
