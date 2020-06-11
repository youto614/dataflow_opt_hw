import java.util.{Properties, UUID}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

class flink_to_S3 (accessKey: String, secretKey: String, endpoint: String, bucket: String, upkey: String, period: Int,inputTopic: String, bootstrapServers: String){


  def up_to_S3 {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", bootstrapServers)
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new FlinkKafkaConsumer010[String](inputTopic,
      new SimpleStringSchema, kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputKafkaStream = env.addSource(kafkaConsumer)

    val result= inputKafkaStream.flatMap {
      _.toLowerCase
        .split("[{}]")
        .filter ( _.contains("username"))
        .filter ( _.contains("buy_time"))
        .filter ( _.contains("buy_address"))
        .filter ( _.contains("origin"))
        .filter ( _.contains("destination"))

    }

      //get destination
    .map(x =>{( x.split(",")(4).split("\":")(1).replace("\"","").replace(" ",""),"{"+x+"}")})

      //result.print()
    result.writeUsingOutputFormat(new S3Writer(accessKey, secretKey, endpoint, bucket, upkey, period))
    env.execute()
  }
}
