import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.test.OutputVerifier
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.Properties
import org.junit.Assert.assertEquals


class BasicSpec {
  var testDriver: TopologyTestDriver = _
  val stringSerializer = new StringSerializer
  val recordFactory = new Nothing(stringSerializer, stringSerializer)

  @Before def setUpTopologyTestDriver(): Unit = {
    val config = new Properties
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    val wordCountApp = new Nothing
    val topology = wordCountApp.createTopology
    testDriver = new Nothing(topology, config)
  }

  @After def closeTestDriver(): Unit = {
    testDriver.close
  }

  def pushNewInputRecord(value: String): Unit = {
    testDriver.pipeInput(recordFactory.create("word-count-input", null, value))
  }

  @Test def dummyTest(): Unit = {
    val dummy = "Du" + "mmy"
    assertEquals(dummy, "Dummy")
  }

  def readOutput: ProducerRecord[String, Long] = testDriver.readOutput("word-count-output", new StringDeserializer, new LongDeserializer)

  @Test def makeSureCountsAreCorrect(): Unit = {
    val firstExample = "testing Kafka Streams"
    pushNewInputRecord(firstExample)
    OutputVerifier.compareKeyValue(readOutput, "testing", 1L)
    OutputVerifier.compareKeyValue(readOutput, "kafka", 1L)
    OutputVerifier.compareKeyValue(readOutput, "streams", 1L)
    assertEquals(readOutput, null)
    val secondExample = "testing Kafka again"
    pushNewInputRecord(secondExample)
    OutputVerifier.compareKeyValue(readOutput, "testing", 2L)
    OutputVerifier.compareKeyValue(readOutput, "kafka", 2L)
    OutputVerifier.compareKeyValue(readOutput, "again", 1L)
  }

  @Test def makeSureWordsBecomeLowercase(): Unit = {
    val upperCaseString = "KAFKA kafka Kafka"
    pushNewInputRecord(upperCaseString)
    OutputVerifier.compareKeyValue(readOutput, "kafka", 1L)
    OutputVerifier.compareKeyValue(readOutput, "kafka", 2L)
    OutputVerifier.compareKeyValue(readOutput, "kafka", 3L)
  }
}
