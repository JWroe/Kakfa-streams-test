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
        for (CWTEvent mergedEvent : MergedEvents) {
            if (mergedEvent.matches(event)) {
                matches++;
                mergedEvent.update(event);
            }
        }

        if (matches == 0) {
            MergedEvents.add(event);
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