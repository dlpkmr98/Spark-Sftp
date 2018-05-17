package com.org.dilip.sparksftp

import org.scalatest.Sequential
import com.org.dilip.sparksftp.commons.PropertiesSpec
import com.org.dilip.sparksftp.commons.UtilitiesSpec
import com.org.dilip.sparksftp.jobs.SftpStreamSpec
import com.org.dilip.sparksftp.jobs.SftpMultiFileWriterSpec

//use when need tor run sequentially 
class StepSuites extends Sequential(
    new MainStreamSpec,
    new PropertiesSpec,
    new UtilitiesSpec,
    new SftpStreamSpec,
    new SftpMultiFileWriterSpec
)
  