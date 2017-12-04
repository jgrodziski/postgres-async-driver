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

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import java.net.InetSocketAddress;

@Value
@Accessors(fluent = true)
@Builder
public class DatabaseConfig {
    InetSocketAddress address;
    String username;
    String password;
    String database;
    boolean pipeline;
    int poolSize;
    int connectTimeout;
    int statementTimeout;
    int poolCloseTimeout;
    boolean useSsl;
}
