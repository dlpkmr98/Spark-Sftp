package com.org.dilip.sparksftp.jobs

import scala.util.control.Exception.Finally
import scala.util.control.Exception.Finally
import org.apache.spark.sql.DataFrame
import com.org.dilip.sparksftp.commons.Utilities
import java.io.InputStream
import java.io.File
import com.jcraft.jsch.ChannelSftp
import scala.util._
import org.apache.commons.io.IOUtils
import com.org.dilip.sparksftp.commons.Properties
import com.org.dilip.pipeline.pipeline.Pipeline
import com.org.dilip.pipeline.pipeline.Transformation
import com.org.dilip.sparksftp.sparksftp._

class SftpMultiFileWriter extends Transformation[Seq[Option[RorD]], Some[String]] with Utilities {

  /* (non-Javadoc)
 * @see com.org.dilip.pipeline.pipeline.Transformation#execute()
 */
override def execute: Seq[Option[RorD]] => Some[String] = dfSeq => {
    require(dfSeq.head.get.isRight,"atleast one dataframe is require")
    dfSeq.foreach(_.get.right.get.show(false))
    lazy val fileSeq = dfSeq.indices.map(_ + Properties.file_name)
    fileSeq.foreach(implicit f => 
    SftpMultiFileWriter execute (SftpStream(f).init(Properties.output_path), createPrivateKey, sshConnection)
    )
    sessionClose
    sftpChannelClose
    Some("")
  }

}

object SftpMultiFileWriter {

  def apply(): SftpMultiFileWriter = new SftpMultiFileWriter()

  /**
     * @param in
     * @param file
     * @param f
     * @param name
     */
  def execute(in:InputStream, file: File, f: File => ChannelSftp)(implicit name: String) = {
    Try { SftpMultiFileWriter.init(in)(name)(f(file)) } match {
      case Success(result) => println("SFTP FINISHED WITH =>" + result)
      case Failure(ex)     => println("Exception Occured " + ex.getMessage)
    }
  }

  /**
   * @param in
   * @param fileName
   * @return
   */
  def init(in: InputStream)(implicit fileName: String) = (sftpChannel: ChannelSftp) => {
      val readData = IOUtils.toBufferedInputStream(in)
      val sftpPath = Properties.sftp_location
      try {
        sftpChannel.cd(sftpPath)
        sftpChannel.put(readData, Properties.currentTime + fileName)
        Try(sftpChannel.chmod(511, fileName))
      } catch { case e: Exception => e.printStackTrace() }
      finally {
        Try(readData.close())
      }
  }
}
