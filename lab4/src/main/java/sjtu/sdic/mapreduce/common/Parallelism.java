package sjtu.sdic.mapreduce.common;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * track whether workers executed in parallel.
 *
 * Created by Cachhe on 2019/4/22.
 */
public class Parallelism {
    public Lock lock = new ReentrantLock();
    public int now;
    public int max;
}
