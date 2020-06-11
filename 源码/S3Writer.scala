import java.io.{File, FileWriter}
import java.util
import java.util.{Timer, TimerTask}

import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import com.bingocloud.{ClientConfiguration, Protocol}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration

class S3Writer(accessKey: String, secretKey: String, endpoint: String, bucket: String, keyPrefix: String, period: Int) extends OutputFormat[(String,String)] {
  var timer: Timer = _
  var file: File = _
  var fileWriter: FileWriter = _
  var length = 0L
  var amazonS3: AmazonS3Client = _
  var destination: util.ArrayList[String] =new util.ArrayList[String]

  def upload: Unit = {
    this.synchronized {
      if (length > 0) {
        //fileWriter.close()
      while(!destination.isEmpty) {
        val targetKey = keyPrefix + destination.get(0) + ".txt"
        file=new File(destination.get(0) + ".txt")
        amazonS3.putObject(bucket, targetKey, file)
        println("开始上传文件：%s 至 %s 桶的 %s 目录下".format(file.getAbsoluteFile, bucket, targetKey))
        destination.remove(0)
      }
        file = null
        fileWriter = null
        length = 0L
      }
    }
  }

  override def configure(configuration: Configuration): Unit = {
    timer = new Timer("S3Writer")
    timer.schedule(new TimerTask() {
      def run(): Unit = {
        upload
      }
    }, 1000, period)
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)

  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {

  }

  override def writeRecord(it: (String,String)): Unit = {
    val temp=it._2
    this.synchronized {
      if (StringUtils.isNoneBlank(temp)) {
        if(!destination.contains(it._1)){
          destination.add(it._1)
        }
        file = new File(it._1 + ".txt")
        fileWriter = new FileWriter(file, true)
        fileWriter.append(temp + "\n")
        length += temp.length
        fileWriter.flush()
        fileWriter.close()
      }
    }
  }


  override def close(): Unit = {
    fileWriter.flush()
    fileWriter.close()
    timer.cancel()
  }
}

