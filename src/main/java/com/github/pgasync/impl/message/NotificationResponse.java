package com.github.pgasync.impl.message;

import lombok.Value;

/**
 * @author Antti Laisi
 */
@Value
public class NotificationResponse implements Message {
    int backend;
    String channel;
    String payload;
}
