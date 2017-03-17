package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.io.*;
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
    public byte[] serialize(String s, Person person) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] yourBytes = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(person);
            out.flush();
            yourBytes = bos.toByteArray();
            bos.close();

        } catch (IOException ex) {
            System.out.println("IO Exception on serializing: " + person);
            System.out.println(ex);
        }
        return yourBytes;
    }

    @Override
    public Person deserialize(String topic, byte[] bytes) {
        if(bytes == null){
            return null;
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        Person person = null;
        try {
            in = new ObjectInputStream(bis);
            Object o = in.readObject();
            in.close();
            person = (Person)o;
        } catch (Exception ex) {
            System.out.print("exception on deserializing person");
            System.out.println(ex);            
        }
        return person;
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