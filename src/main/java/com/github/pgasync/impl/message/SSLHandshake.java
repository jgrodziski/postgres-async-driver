package com.github.pgasync.impl.message;

/**
 * @author Antti Laisi
 */
public enum SSLHandshake implements Message {
    INSTANCE;

    @Override
    public String toString() {
        return "SSLHandshake()";
    }
}
