package com.vsct.vertx.sessionmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.LocalTime;

/**
 * @author Patrice Chalcol
 */
public class IdGenerator {

    private final static Logger log = LoggerFactory.getLogger(IdGenerator.class);

    private IdGenerator() {
    }

    /**
     * Generation de session id.<br>
     * Copie simplifiée de {@code org.apache.catalina.util.StandardSessionIdGenerator#generateSessionId}
     * @return Un id de session
     */
    public static String generateSessionId() {
        LocalTime t1 = LocalTime.now();
        int sessionIdLength = 16;

        SecureRandom random;
        try {
            random = SecureRandom.getInstance("SHA1PRNG");
        } catch (NoSuchAlgorithmException e) {
            log.info("No Such Algorithm : \"SHA1PRNG\"");
            random = new SecureRandom();
        }
        byte bytes[] = new byte[sessionIdLength];
        random.nextBytes(bytes);

        StringBuilder buffer = new StringBuilder(2 * sessionIdLength + 20);

        int resultLenBytes = 0;

        while (resultLenBytes < sessionIdLength) {
            for (int j = 0;
                 j < bytes.length && resultLenBytes < sessionIdLength;
                 j++) {
                byte b1 = (byte) ((bytes[j] & 0xf0) >> 4);
                byte b2 = (byte) (bytes[j] & 0x0f);
                if (b1 < 10)
                    buffer.append((char) ('0' + b1));
                else
                    buffer.append((char) ('A' + (b1 - 10)));
                if (b2 < 10)
                    buffer.append((char) ('0' + b2));
                else
                    buffer.append((char) ('A' + (b2 - 10)));
                resultLenBytes++;
            }
        }

        Duration d = Duration.between(t1, LocalTime.now());
        log.debug("Session id generation duration: " + d.toMillis());

        return buffer.toString();
    }
}
