import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters, StreamsConfig, Topology}
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.Properties
import java.util.concurrent.ThreadLocalRandom

import Basic.properties
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, Materialized, Produced}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}

// Join User purchases (Kstream) to User Dimensions (GlobalKTable)
object UserEnrichment extends App {

  val properties: Properties = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app2")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE) // exactly-once

    // cache is good, but we turn it off in development to see more steps in logs
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    props
  }

  /*
  1 - Read KStream
  2 - Read GlobalKTable
  3 - Inner Join
  4 - Write to Kafka
  5 - Left Join
  6 - Write to Kafka
   */

  implicit val consumed: Consumed[String, String] = Consumed[String, String]
  implicit val grouped: Grouped[String, String] = Grouped[String, String]
  //implicit val materialized: Materialized[String, Long, ByteArrayKeyValueStore] = Materialized[String, Long, ByteArrayKeyValueStore]
  implicit val produced: Produced[String, Long] = Produced[String, Long]

  val builder: StreamsBuilder = new StreamsBuilder // StreamBuilder is entrypoint to the Streams DSL

  val globalKTable: GlobalKTable[String, String] = builder.globalTable("user-table") // requires implicit consumed
  val kStream: KStream[String, String] = builder.stream("user-purcahses")// KStream[K,V]; requires the Consumed Implicit

  val enrichedPurchasesOne =
    kStream
      .join(globalKTable)( // inner join
        (kStreamKey, kStreamValue) => kStreamKey, // map from (k,v) of this stream to the key of the GlobalKTable; remember, globalktables always have a key
        (kStreamValue, globalKTableValue) => "Purchase=" + kStreamValue + ",UserInfo=" + globalKTableValue
      )

  enrichedPurchasesOne.to("inner-join-output")(Produced[String,String])


  val enrichedPurchasesTwo =
    kStream
      .leftJoin(globalKTable)( //there is a chance user-info is null
        (kStreamKey, kStreamValue) => kStreamKey, // map from (k,v) of this stream to the key of the GlobalKTable; remember, globalktables always have a key
        (kStreamValue, globalKTableValue) => {
          if (globalKTableValue == null)
            "Purchase=" + kStreamValue + ",UserInfo=null"
          else
            "Purchase=" + kStreamValue + ",UserInfo=" + globalKTableValue
        }
      )

  enrichedPurchasesTwo.to("left-join-output")(Produced[String,String])

  val topology: Topology = builder.build(properties)

  val kafkaStreams: KafkaStreams = new KafkaStreams(topology, properties)
  kafkaStreams.start()

  // create state store

  val view: ReadOnlyKeyValueStore[String, String] =
  kafkaStreams
    .store(
      StoreQueryParameters
        .fromNameAndType(globalKTable.queryableStoreName, QueryableStoreTypes.keyValueStore())
    )

  view.get("")


  // print out topology
  topology.describe()
  println(kafkaStreams.toString)


  // add shutdown hook; https://kafka.apache.org/25/documentation/streams/developer-guide/write-streams
  Runtime.getRuntime.addShutdownHook(new Thread(() => kafkaStreams.close()))
}
