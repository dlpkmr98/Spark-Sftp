package com.org.dilip.sparksftp

import com.org.dilip.pipeline.pipeline.Pipeline
import java.io.File
import com.org.dilip.sparksftp.commons.Properties
import com.org.dilip.sparksftp.jobs.MultiFileReader
import com.org.dilip.sparksftp.jobs.Formatter
import com.org.dilip.sparksftp.jobs.MultiFileWriter
import com.org.dilip.sparksftp.jobs.SftpMultiFileWriter

class MainStreamSpec extends UnitSpec with MiniSftpServer {

  "The Test" should "test spark-sftp project " in {
    val res = MainStream().init(MainStream.executePipline,MainStream.createPipline, MainStream.getInput)(Array(""))(Properties.input_path)
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
