package com.org.dilip.sparksftp.jobs

import com.org.dilip.sparksftp.UnitSpec
import com.org.dilip.sparksftp.MiniSftpServer
import java.io.File
import com.org.dilip.sparksftp.commons.Properties
import com.org.dilip.sparksftp.MainStream
import scala.reflect.io.Path

class SftpMultiFileWriterSpec extends UnitSpec with MiniSftpServer {
  implicit val fileName = Properties.file_name

  "The Test" should " test init function" in {
    val sftpChannel = SftpMultiFileWriter().sshConnection(new File(""))
    val in = SftpStream("0" + fileName).init("src/test/resources/output/")
    SftpMultiFileWriter.init(in)("0" + fileName)(sftpChannel)
    val file1 = new File("src/test/resources/sftp/" + Properties.currentTime + "0" + fileName)
    file1.exists() shouldBe true
    file1.deleteOnExit()
    sftpChannel.exit()
    SftpMultiFileWriter().sessionClose

  }

  "The Test" should " execute execute function" in {
    val in = SftpStream("0" + fileName).init("src/test/resources/output/")
    val res = SftpMultiFileWriter.execute(in, new File(""), SftpMultiFileWriter().sshConnection)
    val file = new File("src/test/resources/sftp/" + Properties.currentTime+ fileName)
    file.exists() shouldBe true
    file.deleteOnExit()
    SftpMultiFileWriter().sessionClose

  }

}