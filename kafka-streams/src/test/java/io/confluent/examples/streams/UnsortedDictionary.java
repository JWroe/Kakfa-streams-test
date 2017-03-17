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

public class UnsortedDictionary  {

}