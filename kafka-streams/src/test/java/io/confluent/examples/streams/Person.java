package io.confluent.examples.streams;

import java.io.*;
import java.util.*;

public class Person implements Serializable {
    private static final long serialVersionUID = 7526472295622776112L;

    public Person() {
    }

    public Person withCWTEvent(CWTEvent event) {
        if (NhsNumber == null) {
            NhsNumber = event.NhsNumber;
        }

        int matches = 0;
        //todo: make sure we recalculate merged events if an our of order CWTEvent comes in
        for (CWTEvent mergedEvent : MergedEvents) {
            if (mergedEvent.matches(event)) {
                matches++;
                mergedEvent.update(event);
            }
        }

        if (matches == 0) {
            CWTEvent merged = new CWTEvent(event); //creating a new event so it gets a new id
            MergedEvents.add(merged);
        }
        if (matches > 1) {
            //error case
        }

        return this;
    }

    List<CWTEvent> CWTEvents = new ArrayList<>();
    List<CWTEvent> MergedEvents = new ArrayList<>();

    public String NhsNumber;
}