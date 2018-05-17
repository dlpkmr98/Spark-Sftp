package com.org.dilip.sparksftp.commons

import com.jcraft.jsch.ChannelSftp
import java.util.Scanner
import com.org.dilip.sparksftp.UnitSpec
import com.org.dilip.sparksftp.MiniSftpServer
import java.io.File
import com.org.dilip.sparksftp.jobs.SftpMultiFileWriter

class UtilitiesSpec extends UnitSpec with MiniSftpServer {

  "The Test" should " test createPrivateKey functionality" in {

    val res = SftpMultiFileWriter().createPrivateKey
    res.isInstanceOf[File] shouldBe true

  }

  "The Test" should " test sshConnection functionality" in {

    val ref = SftpMultiFileWriter().sshConnection(new File(""))
    ref.isInstanceOf[ChannelSftp] shouldBe true

  }

  "The Test" should " test sessionClose functionality" in {
    val ref = SftpMultiFileWriter().sessionClose
    ref.toOption shouldBe None
  }

  "The Test" should " test sftpChannelClose functionality" in {

    val ref = SftpMultiFileWriter().sftpChannelClose
    ref.toOption shouldBe None
  }

  "The Test" should " test writeKey functionality" in {
    val ref = SftpMultiFileWriter().writeKey(new Scanner(""), new File("sshKey"))
    ref shouldBe ()

  }

}