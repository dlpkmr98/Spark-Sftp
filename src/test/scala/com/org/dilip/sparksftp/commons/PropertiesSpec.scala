package com.org.dilip.sparksftp.commons

import com.org.dilip.sparksftp.UnitSpec



/**
  *
  * PropertiesTest class is used to test the properties defined
  * for web dna use case
  *
  */

class PropertiesSpec extends UnitSpec {

  "usre_origin_mapping" should "be correct" in {
    Properties.host should equal("localhost")
    Properties.format should equal ("com.springml.spark.sftp")
    Properties.sftp_location should equal("src/test/resources/sftp/")
    Properties.sftp_user should equal("")
    Properties.sftp_port should equal(22999)
    Properties.sftp_pass should equal("")
    Properties.output_path should equal("src/test/resources/output/")
   
    
  }

}


