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
public class UserSerializer implements Closeable, AutoCloseable, Serializer<User>, Deserializer<User> {
    public static final Charset CHARSET = Charset.forName("UTF-8");

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, User user) {
        String line = String.format(Locale.ROOT, "%s,%s,%s", user.Address, user.Age, user.NhsNumber);
        return line.getBytes(CHARSET);
    }

    @Override
    public User deserialize(String topic, byte[] bytes) {
        try {
            String[] parts = new String(bytes, CHARSET).split(",");
            return new User(parts[2], Integer.parseInt(parts[1]), parts[0]);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    @Override
    public void close() {

    }
}