package com.riferrei.kafka.connect.pulsar;

public abstract class AbstractBasicTest {

    protected final String pulsarVersion = "2.5.0";
    protected final String serviceUrlValue = "pulsar://localhost:6650";
    protected final String serviceHttpUrlValue = "http://localhost:8080";
    protected final String topicPatternValue = "persistent://public/default/.*";

    protected final String[] topic = {
        "topic-1", "topic-2", "topic-3",
        "topic-4", "topic-5", "topic-6"
    };

    protected String listToString(String... topics) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < topics.length; i++) {
            if (i == topics.length - 1) {
                sb.append(topics[i]);
            } else {
                sb.append(topics[i]).append(", ");
            }
        }
        return sb.toString();
    }
    
}
