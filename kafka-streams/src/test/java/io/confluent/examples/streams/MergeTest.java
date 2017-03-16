/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedStream;

import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.validator.PublicClassValidator;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.*;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;


import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;


import java.util.regex.Pattern;


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
    List<Person> inputValues = Arrays.asList(
        new Person("1", 25, "123 fake street"),
        new Person("1", 25, "321 fake street"),
        new Person("1", 25, ""),
        new Person("2", 25, "other street"),
        new Person("1", 30, ""),
        new Person("3", 29, "third street")
    );

     List<KeyValue<String, Person>> expectedMergedUsers = Arrays.asList(
         new KeyValue<>("3", new Person("3", 29, "third street")),   
         new KeyValue<>("2", new Person("2", 25, "other street")),            
         new KeyValue<>("1", new Person("1", 30, "321 fake street"))
     );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();
    final Serde<Person> personSerde = new PersonSerde();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "merge-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,  new PersonSerde().getClass().getName());
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());


    KStreamBuilder builder = new KStreamBuilder();

    KTable<String, Person> people = builder.stream(stringSerde, personSerde, inputTopic)
                                        .groupBy((key, value) -> value.NhsNumber)
                                        .reduce((aggVal, newVal) -> new Person(aggVal.NhsNumber, 
                                                                            nullCoalesce(newVal.Age, aggVal.Age), 
                                                                            getFirstNonEmpty(newVal.Address, aggVal.Address)), "merged-store");

    //users.foreach((key, value) -> System.out.println("\n\ninput - value = " + value));
    // KStream<String,String> sumat = users.grou
    // users.foreach((key, value) -> System.out.println("\n\ninput - key = " + key));

    people.to(stringSerde, personSerde, outputTopic);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
    
    //
    // Step 2: Produce some input data to the input topic.
    //
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerializer.class);
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig);

    //
    // Step 3: Verify the application's output data.
    //
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-lambda-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PersonSerializer.class);
    
    List<KeyValue<String, Person>> actualOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
        outputTopic, expectedMergedUsers.size());
    
    
    streams.close();

    System.out.print("\n\n output -- ");
    actualOutput.foreach((item) -> System.out.print("\n" + item));
  }

  public <T extends Object> T nullCoalesce(T first, T second) {

    System.out.print("\ncomparting " + first + " to " + second + " returns " + first != null ? first : second);
    return first != null ? first : second;
  }

  public String getFirstNonEmpty(String first, String second){
    return first.length() > 0 ? first : second;
  }
}