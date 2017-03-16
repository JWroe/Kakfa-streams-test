package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;

/**
 * (De)serializes SensorReadings from/to strings.
 */
public class PersonSerializer implements Closeable, AutoCloseable, Serializer<Person>, Deserializer<Person> {
    public static final Charset CHARSET = Charset.forName("UTF-8");

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, Person user) {
        String line = String.format(Locale.ROOT, "%s,%s,%s", user.NhsNumber, user.Age, user.Address);
        return line.getBytes(CHARSET);
    }

    @Override
    public Person deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return this.deserialize(new String(bytes, CHARSET));
    }

    public Person deserialize(String input) {
        String[] parts = input.split(",", -1);
        return new Person(parts[0], tryParseInt(parts[1]), parts[2]);
    }

    Integer tryParseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public void close() {

    }
}