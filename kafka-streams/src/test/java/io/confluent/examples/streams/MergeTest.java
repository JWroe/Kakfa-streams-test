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
import java.util.concurrent.ExecutionException;

import java.nio.file.*;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

public class MergeTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String inputTopic = "input-topic";
  private static final String outputTopic = "output-topic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(inputTopic);
    CLUSTER.createTopic(outputTopic);
  }

  @Test
  public void shouldMergeOnNhsNumber() throws Exception {
    final String inputFile = "/tmp/tmpaVzM8e.tmp";
    final String expectedFile = "/tmp/tmpVRmbpa.tmp";

    List<Person> inputValues = new ArrayList<>();

    PersonSerializer personDes = new PersonSerializer();
    try (Stream<String> stream = Files.lines(Paths.get(inputFile))) {
      stream.forEach((line) -> inputValues.add(personDes.deserialize(line)));
    }

    Map<String, Person> expectedOutput = new HashMap<>();
    try (Stream<String> stream = Files.lines(Paths.get(expectedFile))) {
      
      stream.forEach((line) -> updateDict(new KeyValue<String, Person>(personDes.deserialize(line).NhsNumber, personDes.deserialize(line)), expectedOutput));
    }

    System.out.println("expected -- ");
    expectedOutput.forEach((key, val) -> System.out.println(val));
    System.out.println();

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();
    final Serde<Person> personSerde = new PersonSerde();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "merge-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, new PersonSerde().getClass().getName());
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    KStreamBuilder builder = new KStreamBuilder();

    KTable<String, Person> people = builder.stream(stringSerde, personSerde, inputTopic)
        .groupBy((key, value) -> value.NhsNumber)
        .reduce((aggVal, newVal) -> new Person(aggVal.NhsNumber, nullCoalesce(newVal.Age, aggVal.Age), stringCoalesce(newVal.Address, aggVal.Address)), "merged-store");

    //users.foreach((key, value) -> System.out.println("\n\ninput - value = " + value));
    // KStream<String,String> sumat = users.grou
    // users.foreach((key, value) -> System.out.println("\n\ninput - key = " + key));

    people.to(stringSerde, personSerde, outputTopic);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();

    produceInputData(inputValues);

    Thread.sleep(15000);

    List<KeyValue<String, Person>> actualOutput = getOutputData(0);

    Map<String, Person> latest = getJustLatestValues(actualOutput);

    System.out.println("Actual:");
    latest.forEach((key, value) -> System.out.println(value));

    assertThat(latest.values()).containsExactlyElementsOf(expectedOutput.values());

    streams.close();
  }

  public <T1, T2> Map<T1, T2> getJustLatestValues(List<KeyValue<T1, T2>> actualOutput) {
    Map<T1, T2> dict = new HashMap<>();
    actualOutput.forEach((item) -> updateDict(item, dict));
    System.out.println("output count = " + actualOutput.size());
    System.out.println();
    System.out.println("dict count = " + dict.size());

    return dict;
  }

  public <T1, T2> void updateDict(KeyValue<T1, T2> kv, Map<T1, T2> dict) {
    if (dict.containsKey(kv.key))
      dict.replace(kv.key, kv.value);
    else
      dict.put(kv.key, kv.value);
  }

  public <T extends Object> T nullCoalesce(T first, T second) {
    return first != null ? first : second;
  }

  public String stringCoalesce(String first, String second) {
    return first.length() > 0 ? first : second;
  }

  public void produceInputData(List<Person> input) throws InterruptedException, ExecutionException {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerializer.class);

    IntegrationTestUtils.produceValuesSynchronously(inputTopic, input, producerConfig);
  }

  public List<KeyValue<String, Person>> getOutputData(int size) {
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-lambda-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PersonSerializer.class);

    try {
      return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, size, 5000L);
    } catch (InterruptedException e) {
      System.out.println(e);
      return null;
    }
  }
}