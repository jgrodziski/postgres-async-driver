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

import com.github.pgasync.impl.Oid;

/**
 * Converters extend the driver to handle complex data types,
 * for example json or hstore that have no "standard" Java
 * representation.
 *
 * @author Antti Laisi.
 */
public interface Converter<T> {

    /**
     * @return Class to convert
     */
    Class<T> type();

    /**
     * @param o Object to convert, never null
     * @return data in backend format
     */
    byte[] from(T o);

    /**
     * @param oid   Value oid
     * @param value Value in backend format, never null
     * @return Converted object, never null
     */
    T to(Oid oid, byte[] value);

}
