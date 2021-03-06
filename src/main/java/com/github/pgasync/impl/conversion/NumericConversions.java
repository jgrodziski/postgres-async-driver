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

package com.github.pgasync.impl.conversion;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;

import java.math.BigDecimal;
import java.math.BigInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Antti Laisi
 */
enum NumericConversions {
    ;

    static Long toLong(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8:
                return Long.valueOf(new String(value, UTF_8));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Long");
        }
    }

    static Integer toInteger(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4:
                return Integer.valueOf(new String(value, UTF_8));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Integer");
        }
    }

    static Short toShort(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2:
                return Short.valueOf(new String(value, UTF_8));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Short");
        }
    }

    static Byte toByte(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2:
                return Byte.valueOf(new String(value, UTF_8));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Byte");
        }
    }

    static BigInteger toBigInteger(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8:
                return new BigInteger(new String(value, UTF_8));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> BigInteger");
        }
    }

    static BigDecimal toBigDecimal(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8: // fallthrough
            case NUMERIC: // fallthrough
            case FLOAT4: // fallthrough
            case FLOAT8:
                return new BigDecimal(new String(value, UTF_8));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> BigDecimal");
        }
    }

    static Double toDouble(Oid oid, byte[] value) {
        switch (oid) {
            case UNSPECIFIED: // fallthrough
            case INT2: // fallthrough
            case INT4: // fallthrough
            case INT8: // fallthrough
            case NUMERIC: // fallthrough
            case FLOAT4: // fallthrough
            case FLOAT8:
                return Double.valueOf(new String(value, UTF_8));
            default:
                throw new SqlException("Unsupported conversion " + oid.name() + " -> Double");
        }
    }

}
