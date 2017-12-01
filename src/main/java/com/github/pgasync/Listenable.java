package com.github.pgasync;

import rx.Observable;

/**
 * @author Antti Laisi
 */
public interface Listenable {
    /**
     * Returns a stream of notifications on given channel.
     * @param channel channel name
     * @return stream of notifications
     */
    Observable<String> listen(String channel);
}
