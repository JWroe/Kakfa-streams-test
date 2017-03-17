package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.stream.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ExecutionException;

import java.nio.file.*;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

public class MergeTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String inputTopic = "input-topic";
  private static final String personTopic = "person-topic";  
  private static final String mergedEventsTopic = "mergedEventsTopic";
  private static final String breachesTopic = "breachesTopic";
  private static final String rawCWTEventsTopic = "rawCWTEventsTopic";
  
  

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(inputTopic);
    CLUSTER.createTopic(personTopic);
    CLUSTER.createTopic(mergedEventsTopic);
    CLUSTER.createTopic(breachesTopic);
    CLUSTER.createTopic(rawCWTEventsTopic);    
  }

  @Test
  public void shouldMergeOnNhsNumber() throws Exception {
    final String inputFile = "/tmp/tmpTTtjVb.tmp";
    final String expectedFile = "/tmp/tmpOzV9mb.tmp";

    List<CWTEvent> inputValues = new ArrayList<>();

    try (Stream<String> stream = Files.lines(Paths.get(inputFile))) {
      stream.forEach((line) -> inputValues.add(deserialize(line)));
    }

    HashMap<String, CWTEvent> expectedOutput = new LinkedHashMap<>();
    try (Stream<String> stream = Files.lines(Paths.get(expectedFile))) {
      stream.forEach((line) -> expectedOutput.put(deserialize(line).Address, deserialize(line)));
    }

    System.out.println();
    
    System.out.println("expect " + expectedOutput.size() + " records");
    expectedOutput.forEach((k, v) -> System.out.println(v));

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();
    final Serde<CWTEvent> cwtSerde = new CWTEventSerde();
    final Serde<Person> personSerde = new PersonSerde();
    final Serde<BreachData> breachSerde = new GenericSerde<BreachData>();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "merge-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, cwtSerde.getClass().getName());
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
    // streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, Person> people = builder.stream(stringSerde, cwtSerde, inputTopic)
                                            .groupBy((key, cwtEvent) -> cwtEvent.NhsNumber)                         //group incoming data by Nhs number
                                            .aggregate(() -> new Person(),                                          //use incoming data to construct a 'person' object
                                                      (nhsNum, newEvent, person) -> person.withCWTEvent(newEvent),  //add new CWTEvents that come in into this object
                                                      personSerde, "aggregated-person-stream-store")
                                            .toStream()                                                             //convert KTable to a KStream so we can output to a topic
                                            .through(stringSerde, personSerde, personTopic);                        //output to a topic but keep working with the stream

    people.flatMapValues(person -> person.CWTEvents)                 //create a stream of the incoming cwt events enriched with a new unique id
          .to(stringSerde, cwtSerde, rawCWTEventsTopic);             //output to a topic and finish working with the stream

    people.flatMapValues(person -> person.MergedEvents)              //output the updated merged events given the person state - this can create quite a lot of redundant data very quickly so we really need caching on to keep performance good
          .selectKey((nhsNum, cwtEvent) -> cwtEvent.UniqueId)        //use our merged event keys to make sure we only keep the latest state for a given 'merge' 
          .through(stringSerde, cwtSerde, mergedEventsTopic)         //output to a topic but keep working with the stream
          .mapValues(cwtEvent -> cwtEvent.calculateBreachData())     //raise the breach data calculated from the merged record
          .to(stringSerde, breachSerde, breachesTopic);              //output to a topic and finish working with the stream

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();

    produceInputData(inputValues);

    List<KeyValue<String, CWTEvent>> actualOutput = getOutputData(-1);

    System.out.println("Actual: " + actualOutput.size() + " records");    
    // actualOutput.forEach((item) -> System.out.println(item));

    HashMap<String, CWTEvent> latest = getJustLatestValues(actualOutput);

    System.out.println("Just Latest: " + latest.size() + " records");
    // latest.forEach((k, v) -> System.out.println("key: " + k + ", value: " + v));

    assertThat(latest.values()).containsExactlyElementsOf(expectedOutput.values());

    streams.close();
  }

  public <T1, T2> HashMap<T1, T2> getJustLatestValues(List<KeyValue<T1, T2>> actualOutput) {
    HashMap<T1, T2> items = new LinkedHashMap<>();
    actualOutput.forEach((item) -> updateDict(item, items));
    return items;
  }

    public CWTEvent deserialize(String input) {
        String[] parts = input.split(",", -1);
        return new CWTEvent(parts[0], tryParseInt(parts[1]), parts[2], parts[3],parts[4],parts[5]);
    }

    Integer tryParseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
  public <T1, T2> void updateDict(KeyValue<T1, T2> kv, HashMap<T1, T2> uniques) {
    if (uniques.containsKey(kv.key)) {
      uniques.remove(kv.key);
    } else {
    }

    uniques.put(kv.key, kv.value);
  }

  public void produceInputData(List<CWTEvent> input) throws InterruptedException, ExecutionException {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CWTEventSerializer.class);

    IntegrationTestUtils.produceValuesSynchronously(inputTopic, input, producerConfig);
  }

  public List<KeyValue<String, CWTEvent>> getOutputData(int size) {
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-lambda-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CWTEventSerializer.class);

    try {
      return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, mergedEventsTopic, size, 900000L);
    } catch (InterruptedException e) {
      System.out.println(e);
      return null;
    }
  }
}