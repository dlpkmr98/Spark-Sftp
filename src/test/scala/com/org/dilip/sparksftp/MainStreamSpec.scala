package com.org.dilip.sparksftp

import com.org.dilip.pipeline.pipeline.Pipeline
import java.io.File
import com.org.dilip.sparksftp.commons.Properties

class MainStreamSpec extends UnitSpec with MiniSftpServer {

  "The Test" should "test spark-sftp project " in {
    val res = MainStream() init MainStream.pipeline
    res.isInstanceOf[Unit] shouldBe true
    implicit val fileName = Properties.file_name
    val file1 = new File("src/test/resources/sftp/" + Properties.currentTime + "0" + fileName)
    val file2 = new File("src/test/resources/sftp/" + Properties.currentTime + "1" + fileName)
    file1.exists() shouldBe true
    file2.exists() shouldBe true
    file1.deleteOnExit()
    file2.deleteOnExit()

  }

}