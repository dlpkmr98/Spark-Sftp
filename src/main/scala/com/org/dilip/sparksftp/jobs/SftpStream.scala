package com.org.dilip.sparksftp.jobs

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import java.io.InputStream
import com.org.dilip.sparksftp.commons.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

class SftpStream(implicit fileName:String) {

  /**
 * @param df_save_path
 * @return
 */
def init(df_save_path: String): InputStream = {
      val uri = Properties.hdfs_url + df_save_path + fileName + "/part-00000"
      val hdfsConf = new Configuration
      val fs = FileSystem.get(URI.create(uri), hdfsConf)
      var in: FSDataInputStream = null
      try {
        in = fs.open(new Path(uri))
      } catch {
        case ex: Exception => throw ex
      }
      in
  }
}

object SftpStream {
  def apply(implicit fileName:String) = new SftpStream
  def apply(in: InputStream): InputStream = in

}