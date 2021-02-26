import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StoreQueryParameters, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Materialized, Produced}
import java.util.Properties

import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}


object FavouriteColor extends App {

  // topic is comma delimited: name,fav_color
  // get a running count of most up-to-date fav_color and write

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

  implicit val longSerde: Serde[Long] = Serdes.serdeFrom(classOf[Long])
  implicit val consumed: Consumed[String, String] = Consumed[String, String]
  implicit val grouped: Grouped[String, String] = Grouped[String, String]
  //implicit val materialized: Materialized[String, Long, ByteArrayKeyValueStore] = Materialized[String, Long, ByteArrayKeyValueStore]
  implicit val produced: Produced[String, Long] = Produced[String, Long]

  /*
    1 - Read one topic from Kafka
    2 - Filter bad values
    3 - SelectKey that will be the user_id
    4 - MapValues to extract the colors as lowercase
    5 - Filter to remove bad colors
    6 - write to kafka as intermediary topic
    7 - read from Kafka as KTable
    8 - GroupBy colors
    9 - count to count occurences
    10 - write to kafka as final topic
   */

  val builder: StreamsBuilder = new StreamsBuilder // StreamBuilder is entrypoint to the Streams DSL

  val usersAndColors = builder // StreamsBuilder
    .stream("fav-color")// KStream[K,V]; requires the Consumed Implicit
    .filter((_, v) => v.contains(","))
    .selectKey((_, v) => v.split(",").head)
    .mapValues(v => v.split(",").last.toLowerCase) // probably faster if do it in one map step
    .filter((user,color) => Array("red", "green", "blue").contains(color))

  usersAndColors.to("user-keys-and-colors")(Produced[String, String])

  builder.table("user-keys-and-colors")

  // read that topic as a KTable so that updates are treated as upserts
  val usersAndColorsTable = builder.table("users-keys-and-colors")

  // counts by color
  val favouriteColors =
    usersAndColorsTable
      .groupBy((user, color) => (color, color))
      .count()(Materialized.as("some-store")(Serdes.StringSerde, longSerde))


  favouriteColors.toStream.to("output-topic")

  val topology: Topology = builder.build(properties)

  val streams = new KafkaStreams(topology, properties)


  streams.cleanUp() // do this in dev, NOT in prod
  streams.start()

  // need to
  val view: ReadOnlyKeyValueStore[String, Long] =
    streams
      //     .store("", QueryableStoreTypes.keyValueStore())
      .store(
        StoreQueryParameters
          .fromNameAndType(favouriteColors.queryableStoreName, QueryableStoreTypes.keyValueStore())
      )

  view.get("")


  //print topology
  println(streams.toString)


  // add shutdown hook; https://kafka.apache.org/25/documentation/streams/developer-guide/write-streams
  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))
}
