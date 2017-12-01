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

import rx.Completable;
import rx.Single;

import java.util.concurrent.TimeUnit;

/**
 * A single physical connection to PostgreSQL backend.
 *
 * @author Antti Laisi
 */
public interface Connection extends Db {
    /**
     * Closes the connection.
     */
    Completable close();

    /**
     * Sets statement timeout on the connection and returns it.
     * @param timeout  timeout value
     * @param timeUnit time unit
     * @return returns connection with statement timeout set
     */
    Connection withTimeout(long timeout, TimeUnit timeUnit);

    /**
     * Tells if connection is open. If a connection is closed it cannot be used anymore.
     * @return true if open false otherwise
     */
    boolean isConnected();
}
