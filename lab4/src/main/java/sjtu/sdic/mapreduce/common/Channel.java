package sjtu.sdic.mapreduce.common;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This is a simple Java-simulation of chan in Go-lang
 *
 * Created by Cachhe on 2019/4/19.
 */
public class Channel<T> {
    private BlockingQueue<T> queue;

    public Channel() {
        queue = new LinkedBlockingQueue<>();
    }

    /**
     * put a value to this channel
     *
     * @throws InterruptedException this could be ignored
     */
    public void write(T t) throws InterruptedException {
        queue.put(t);
    }

    /**
     * return only if there's a value, otherwise blocking
     *
     * @return value
     * @throws InterruptedException interruptedException
     */
    public T read() throws InterruptedException {
        return queue.take();
    }

    /**
     * wrapper for {@link BlockingQueue#poll()}
     * @return value or null if no value, without blocking
     */
    public T poll() {
        return queue.poll();
    }
}
