package com.org.dilip.sparksftp.commons

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.commons.io.IOUtils
import com.jcraft.jsch._
import java.io._
import java.util.{ Calendar, Scanner }
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.fs.LocalFileSystem
import scala.util.Try

/**
 * @author dilip
 *
 */
trait Utilities {

  var session: Session = null
  var sftpChannel: ChannelSftp = null

  /**
   * @return
   */
  def sshConnection = (f: File) => {
    try {
      val jsch = new JSch()
      Try(jsch.addIdentity(f.getAbsolutePath))
      session = jsch.getSession(Properties.sftp_user, Properties.host, Properties.sftp_port)
      session.setConfig("StrictHostKeyChecking", "no")
      session.connect()
      sftpChannel = session.openChannel("sftp").asInstanceOf[ChannelSftp]
      sftpChannel.connect()
      sftpChannel
    } catch {
      case e: JSchException => throw e

    }
  }

  /**
   * @return
   */
  def createPrivateKey: File = {
//    val privateKey: File = new File("sshKey")
//    val path: Path = new Path(Properties.sshKeyFilePath)
//    val fileSystem: FileSystem = FileSystem.get(getConfiguration(Properties.hdfs_url))
//    if (fileSystem.exists(path))
//      writeKey(new Scanner(fileSystem.open(path)), privateKey)
//    privateKey
    new File("")
  }

  def writeKey(scanner: Scanner, key: File) = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(key)))
    while (scanner.hasNextLine) {
      bw.write(scanner.nextLine)
      bw.newLine()
    }
    bw.close()
  }

  /**
   * @param hdfsPath
   * @return
   */
  def getConfiguration(hdfsPath: String): Configuration = {
    val conf = new Configuration
    conf.set("fs.defaultFS", hdfsPath)
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    return conf

  }

  /**
   *
   */
  def sessionClose = Try(session.disconnect())
  def sftpChannelClose = Try(sftpChannel.exit())

}