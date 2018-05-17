package com.org.dilip.sparksftp

import org.scalatest._
import org.apache.sshd.SshServer
import org.apache.sshd.server.PasswordAuthenticator
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.CommandFactory
import org.apache.sshd.server.command.ScpCommandFactory
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.server.UserAuth
import org.apache.sshd.common.NamedFactory
import org.apache.sshd.server.auth.UserAuthNone
import scala.collection.JavaConverters._
import org.apache.sshd.server.Command
import org.apache.sshd.server.sftp.SftpSubsystem
import scala.collection.mutable.ListBuffer

trait MiniSftpServer extends SuiteMixin with BeforeAndAfterAll with UnitSpecTrait {
  this: Suite =>

  val sshd = SshServer.setUpDefaultServer()
  sshd.setPort(22999)
  sshd.setHost("localhost");
  sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider("hostkey.ser"))
  val userAuthFactories = new ListBuffer[NamedFactory[UserAuth]]().asJava
  userAuthFactories.add(new UserAuthNone.Factory())
  sshd.setUserAuthFactories(userAuthFactories)
  sshd.setCommandFactory(new ScpCommandFactory())
  val namedFactoryList = new ListBuffer[NamedFactory[Command]]().asJava
  namedFactoryList.add(new SftpSubsystem.Factory())
  sshd.setSubsystemFactories(namedFactoryList)

  override def beforeAll() {
    println("Start SFTP======")
    sshd.start()
  }

  override def afterAll() {
    println("Stop SFTP======")
    sshd.stop()

  }

}

