/**

    Copyright © 2020 Ricardo Ferreira (riferrei@riferrei.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

*/

package com.riferrei.kafka.connect.pulsar;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VersionUtil {

    private static Logger log = LoggerFactory.getLogger(VersionUtil.class);
    private static String versionPropsFile = "/kafka-connect-pulsar.properties";
    private static String version = "unknown";

    static {
        try (InputStream stream = VersionUtil.class.getResourceAsStream(versionPropsFile)) {
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

    private VersionUtil() {
    }

}
