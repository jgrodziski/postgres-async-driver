/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync;

import com.github.pgasync.DatabaseConfig.DatabaseConfigBuilder;
import com.github.pgasync.impl.PgConnectionPool;
import com.github.pgasync.impl.conversion.DataConverter;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Builder for creating {@link ConnectionPool} instances.
 *
 * @author Antti Laisi
 */
public class ConnectionPoolBuilder {
    private static final int DEFAULT_PORT = 5432;
    private static final int DEFAULT_POOL_SIZE = 4;
    private static final long DEFAULT_CONNECT_TIMEOUT = 30000;

    private final List<Converter<?>> converters = new ArrayList<>();
    private DatabaseConfigBuilder configBuilder = DatabaseConfig.builder();
    private String hostname;
    private int port = DEFAULT_PORT;

    public ConnectionPoolBuilder() {
        poolSize(DEFAULT_POOL_SIZE);
        connectTimeout(DEFAULT_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * @return Pool ready for use
     */
    public ConnectionPool build() {
        DatabaseConfig config = configBuilder
                .address(InetSocketAddress.createUnresolved(hostname, port))
                .build();

        DataConverter dataConverter = new DataConverter(converters);
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);

        return new PgConnectionPool(config, dataConverter, eventLoopGroup);
    }

    public ConnectionPoolBuilder hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public ConnectionPoolBuilder port(int port) {
        this.port = port;
        return this;
    }

    public ConnectionPoolBuilder username(String username) {
        configBuilder.username(username);
        return this;
    }

    public ConnectionPoolBuilder password(String password) {
        configBuilder.password(password);
        return this;
    }

    public ConnectionPoolBuilder database(String database) {
        configBuilder.database(database);
        return this;
    }

    public ConnectionPoolBuilder poolSize(int poolSize) {
        configBuilder.poolSize(poolSize);
        return this;
    }

    public ConnectionPoolBuilder converters(Converter<?>... converters) {
        Collections.addAll(this.converters, converters);
        return this;
    }

    public ConnectionPoolBuilder ssl(boolean ssl) {
        configBuilder.useSsl(ssl);
        return this;
    }

    public ConnectionPoolBuilder pipeline(boolean pipeline) {
        configBuilder.pipeline(pipeline);
        return this;
    }

    public ConnectionPoolBuilder connectTimeout(long value, TimeUnit timeUnit) {
        configBuilder.connectTimeout((int) timeUnit.toMillis(value));
        return this;
    }

    public ConnectionPoolBuilder statementTimeout(long value, TimeUnit timeUnit) {
        configBuilder.statementTimeout((int) timeUnit.toMillis(value));
        return this;
    }
}