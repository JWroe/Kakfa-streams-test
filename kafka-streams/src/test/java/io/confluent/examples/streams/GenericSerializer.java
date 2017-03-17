package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.io.*;
import java.util.Map;

public class GenericSerializer<T> implements Closeable, AutoCloseable, Serializer<T>, Deserializer<T> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, T person) {
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
    public T deserialize(String topic, byte[] bytes) {
        if(bytes == null){
            return null;
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        T deserialised = null;
        try {
            in = new ObjectInputStream(bis);
            Object o = in.readObject();
            in.close();
            deserialised = (T)o;
        } catch (Exception ex) {
            System.out.print("exception on deserializing person");
            System.out.println(ex);            
        }
        return deserialised;
    }

    @Override
    public void close() {

    }
}

