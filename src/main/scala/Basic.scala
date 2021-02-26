import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Materialized, Produced}

object Basic extends App {

  val properties: Properties = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])

    props
  }

  implicit val consumed: Consumed[String, String] = Consumed[String, String]
  implicit val grouped: Grouped[String, String] = Grouped[String, String]
  implicit val materialized: Materialized[String, Long, ByteArrayKeyValueStore] = Materialized[String, Long, ByteArrayKeyValueStore]
  implicit val produced: Produced[String, Long] = Produced[String, Long]

  val builder: StreamsBuilder = new StreamsBuilder // StreamBuilder is entrypoint to the Streams DSL

  builder // StreamsBuilder
    .stream("word-count-input")// KStream[K,V]; requires the Consumed Implicit
    .mapValues(v => v.toLowerCase) //KStream[K,V]
    .flatMapValues(v => v.split(" "))
    .selectKey((_, v) => v) // make value into key
    .groupByKey // KGroupedStream[K, V] ; requires grouped implicit
    .count // KTable[K, Long] ; requires materialized implicit; Ktable is a kind of caching
    .toStream
    .to("word-count-output") //Unit ; requires implicit Produced or implicit serdes



  val topology: Topology = builder.build(properties)

  val kafkaStreams: KafkaStreams = new KafkaStreams(topology, properties)
  kafkaStreams.start()
  // print out topology
  topology.describe()
  println(kafkaStreams.toString)


  // add shutdown hook; https://kafka.apache.org/25/documentation/streams/developer-guide/write-streams
  Runtime.getRuntime.addShutdownHook(new Thread(() => kafkaStreams.close()))
}
