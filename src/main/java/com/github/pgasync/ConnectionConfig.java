package com.github.pgasync;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import java.net.InetSocketAddress;

@Value
@Accessors(fluent = true)
@Builder
public class ConnectionConfig {
    InetSocketAddress address;
    String username;
    String password;
    String database;
    boolean pipeline;
    int poolSize;
    int connectTimeout;
    boolean useSsl;
}
