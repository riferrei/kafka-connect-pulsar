package com.riferrei.kafka.connect.pulsar.util;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version {

    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String PATH = "/kafka-connect-pulsar.properties";
    private static String version = "unknown";

    static {
        try (InputStream stream = Version.class.getResourceAsStream(PATH)) {
          Properties props = new Properties();
          props.load(stream);
          version = props.getProperty("version", version).trim();
        } catch (Exception ex) {
          log.warn("Error while loading version: ", ex);
        }
    }

    public static String getVersion() {
        return version;
    }

}
