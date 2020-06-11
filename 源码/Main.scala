import  scala.io.StdIn
object Main {
  //s3accessKey
  val accessKey = "6FB9ECFF3FE0B0670B93"
  //s3secretKey
  val secretKey = "WzlDMUQ2QzhEN0MwOEE4QkNGRkVCNzlDNTAzRDg3NDRERjY3NjU2NTVd"
  //s3地址
  val endpoint = "scuts3.depts.bingosoft.net:29999"
  //s3上传到的桶
  val bucket = "act3"
  //s3上传文件的路径前缀
  val upkey = "upload/"
  //上传数据间隔 单位毫秒
  val period = 5000
  //kafka主题名称
  val Topic = "mn_buy_ticket_test1591"
  //kafka地址
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"
  //s3要读取的文件
  val readkey = "data/daas.txt"

  def exe(): Unit ={
    var order:String=""
    var flag = true
    while (flag) {
      order = StdIn.readLine()
      if (order == "1") {
        val SToK = new S3_to_kafka(accessKey, secretKey, endpoint, bucket, readkey, Topic, bootstrapServers)
        val s3Content = SToK.readFile()
        SToK.produceToKafka(s3Content)
      } else if (order == "2") {
        val showK = new show_kafka_by_flink(Topic, bootstrapServers)
        showK.show
      } else if (order == "3") {
        val fToS = new flink_to_S3(accessKey, secretKey, endpoint, bucket, upkey, period, Topic, bootstrapServers)
        fToS.up_to_S3
      }else if (order == "4"){
        flag=false
      } else {
        println("指令无效，请重新输入！")
      }
    }
  }
  def get_order: Unit ={
    println("请输入您要执行的任务编号")
    val order1="1.接入S3的数据"
    val order2="2.flink对接kafka并输出"
    val order3="3.按destination进行归类并储存在S3中"
    val order4="4.退出程序"
    println(s"$order1    ｜$order2    ｜$order3    ｜$order4")
    exe
  }

  def main(args: Array[String]): Unit = {
    get_order
    println("程序正常关闭！")
  }
}
