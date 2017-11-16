package com.github.pgasync.impl.message;

import lombok.Value;
import lombok.experimental.Accessors;

/**
 * @author Antti Laisi
 */
@Value
@Accessors(fluent = true)
public class NotificationResponse implements Message {
    int backend;
    String channel;
    String payload;
}
