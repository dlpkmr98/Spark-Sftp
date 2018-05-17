package com.org.dilip.sparksftp.commons

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.util.Calendar

object Properties {

  lazy val conf = ConfigFactory.load("spark-sftp")
  lazy val input_path = conf.getString("hdfs_path.input_path")
  lazy val output_path = conf.getString("hdfs_path.save_path")
  lazy val host = conf.getString("sftp.host")
  lazy val format = conf.getString("sftp.format")
  lazy val sftp_location = conf.getString("sftp.sftp_location")
  lazy val sftp_user = conf.getString("sftp.sftp_user")
  lazy val sftp_port = conf.getInt("sftp.sftp_port")
  lazy val sftp_pass = conf.getString("sftp.sftp_pass")
  lazy val hdfs_url = conf.getString("sftp.hdfs_url")
  lazy val sshKeyFilePath = conf.getString("parameters.sshKeyFilePath")
  val formater = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss")
  lazy val currentTime = formater.format(Calendar.getInstance.getTime)
  lazy val appName = conf.getString("application.name")
  lazy val master = conf.getString("application.master")
  lazy val file_name = conf.getString("sftp.file_name")
}
