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
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.validator.PublicClassValidator;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.*;


import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import junit.framework.Assert;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how aggregations on a KTable produce the expected
 * results even though new data is continuously arriving in the KTable's input topic in Kafka.
 *
 * The use case we implement is to continuously compute user counts per region based on location
 * updates that are sent to a Kafka topic.  What we want to achieve is to always have the
 * latest and correct user counts per region even as users keep moving between regions while our
 * stream processing application is running (imagine, for example, that we are tracking passengers
 * on air planes).  More concretely,  whenever a new messages arrives in the Kafka input topic that
 * indicates a user moved to a new region, we want the effect of 1) reducing the user's previous
 * region by 1 count and 2) increasing the user's new region by 1 count.
 *
 * You could use the code below, for example, to create a real-time heat map of the world where
 * colors denote the current number of users in each area of the world.
 *
 * This example is related but not equivalent to {@link UserRegionLambdaExample}.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
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
    List<KeyValue<String, User>> people = Arrays.asList(
        //new KeyValue<>("1", new User("1", 25, null)),
        new KeyValue<>("1", new User("1", 25, "123 fake street"))
    );

    List<KeyValue<String, User>> expectedMergedUsers = Arrays.asList(
        new KeyValue<>("1", new User("1", 25, "123 fake street"))
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();
    final Serde<User> userSerde = new UserSerde();

    User user = new User("1", 25, "123 fake street");

    Serializer<User> ser = userSerde.serializer();

    byte[] bytes = ser.serialize(null, user);
    User deserUser = userSerde.deserializer().deserialize(null, bytes);

    System.out.print("\n nhs num:" + user.NhsNumber);
    System.out.print("\n age:" + user.Age);

    System.out.print("\n address:" + user.Address);
    System.out.print("\n\n");
    
    Assert.assertEquals(user.Address, deserUser.Address);
    Assert.assertEquals(user.Age, deserUser.Age);
    Assert.assertEquals(user.NhsNumber, deserUser.NhsNumber);
    
  }
  class UserSerde implements Serde<User>{

    @Override
      
     public  void configure(Map<String, ?> configs, boolean isKey){}

    /**
     * Close this serde class, which will close the underlying serializer and deserializer.
     * This method has to be idempotent because it might be called multiple times.
     */
    @Override
    public void close(){

    }

    @Override
    public Serializer<User> serializer(){
        return new UserSerializer();
    }

    @Override
    public Deserializer<User> deserializer(){
        return new UserSerializer();
    }
  }
}