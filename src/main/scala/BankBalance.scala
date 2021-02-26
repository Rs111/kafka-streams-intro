


import java.time.Instant

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.connect.json.{JsonDeserializer, JsonSerializer}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StoreQueryParameters, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder, KStre}
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Materialized, Produced}
import java.util.Properties

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.apache.kafka.streams.state.QueryableStoreTypes

object BankBalance extends App {

  val jsonSerializer: JsonSerializer = new JsonSerializer()
  val jsonDeserializer: JsonDeserializer = new JsonDeserializer()
  val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)

  val properties: Properties = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app2")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes[JsonNode]])
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE) // exactly-once

    // cache is good, but we turn it off in development to see more steps in logs
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    props
  }

  // Topic
  // k=customer, v={"Name": "John", "amount": 123, "time": "2017-07-19T05:24:52}

  /*
    1 - read one topic
    2 - GroupByKey, because your topic already has the right key! no repartition happens
    3 - aggregate to compute bank balance
    4 - to(output-topic)
   */

  val builder: StreamsBuilder = new StreamsBuilder // StreamBuilder is entrypoint to the Streams DSL

  val bankTransactions =
    builder
      .stream("bank-transactions")(Consumed.`with`(Serdes.String, jsonSerde))

  val initialBalance: ObjectNode = JsonNodeFactory.instance.objectNode()
  initialBalance.put("count", 0)
  initialBalance.put("balance", 0)
  initialBalance.put("time", Instant.ofEpochMilli(0).toString)




  bankTransactions
    .groupByKey(Grouped.`with`(Serdes.String, jsonSerde))
    .aggregate(initialBalance.asInstanceOf[JsonNode])(
      (key, transaction, balance) => newBalance(transaction, balance)
    )(Materialized.as("store-name")(Serdes.String, jsonSerde))


  def newBalance(transaction: JsonNode, balance: JsonNode): JsonNode = {
    val newBalance = JsonNodeFactory.instance.objectNode()
    newBalance.put("count", balance.get("count").asInt + 1)
    newBalance.put("balance", balance.get("balance").asInt + transaction.get("amount").asInt)

    val balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli
    val transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli
    val newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch))
    newBalance.put("time", newBalanceInstant.toString)
    newBalance
  }












}
