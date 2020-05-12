package sjtu.sdic.mapreduce.core;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import sjtu.sdic.mapreduce.common.Channel;
import sjtu.sdic.mapreduce.common.Func;
import sjtu.sdic.mapreduce.common.JobPhase;
import sjtu.sdic.mapreduce.common.Utils;
import sjtu.sdic.mapreduce.rpc.Call;
import sjtu.sdic.mapreduce.rpc.MasterRpcService;
import sjtu.sdic.mapreduce.rpc.WorkerRpcService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static sjtu.sdic.mapreduce.common.Utils.debug;

/**
 * Master holds all the state that the master needs to keep track of.
 *
 * Created by Cachhe on 2019/4/19.
 */
public class Master implements MasterRpcService {
    public static final int MASTER_PORT = 12200;
    private Lock lock;

    public String address; // stands for name in sequential, and ip for distributed
    public Channel<Boolean> doneChannel; // doneChannel chan bool

    // protected by the lock
    private Condition newCond; // signals when Register() adds to workers
    private List<String> workers; // workers' addresses (names)

    // per-task information
    private String jobName;
    public String[] files;
    private int nReduce;

    private ProviderConfig<MasterRpcService> rpc;
    private boolean isExported;

    public List<Integer> stats;

    private Master(String master) {
        address = master;
        // shutdown
        lock = new ReentrantLock();
        newCond = lock.newCondition();
        // done
        doneChannel = new Channel<>();
        workers = new ArrayList<>();
    }



    /**
     * Sequential runs map and reduce tasks sequentially, waiting for each task to
     * complete before running the next.
     *
     * @param jobName the job name, which affects the output file's name
     * @param files files' name (if in same dir, it's also the files' path)
     * @param nReduce the number of reduce task that will be run ("R" in the paper)
     * @param mapF user-defined map function
     * @param reduceF user-defined reduce function
     * @return master instance
     */
    public static Master sequential(String jobName, String[] files, int nReduce, MapFunc mapF, ReduceFunc reduceF) {
        Master mr = new Master("master");
        new Thread(() -> {
            mr.run(jobName, files, nReduce, jobPhase -> {
                switch (jobPhase) {
                    case MAP_PHASE:
                        for (int i = 0; i < files.length; i++) {
                            Mapper.doMap(mr.jobName, i, files[i], mr.nReduce, mapF);
                        }
                        break;
                    case REDUCE_PHASE:
                        for (int i = 0; i < nReduce; i++) {
                            Reducer.doReduce(mr.jobName, i, Utils.mergeName(mr.jobName, i), mr.files.length, reduceF);
                        }
                        break;
                }
                return null;
            }, aVoid -> {
                mr.stats = Collections.singletonList(files.length + nReduce);
                return null;
            });
        }).start();
        return mr;
    }

    /**
     * register is an RPC method that is called by workers after they
     * have started up to report that they are ready to receive tasks.
     *
     * @param worker worker's address (name)
     */
    @Override
    public void register(String worker) {
        try {
            lock.lock();
            debug(String.format("Register: worker %s", worker));
            if (workers == null) workers = new ArrayList<>();
            workers.add(worker);

            // tell forwardRegistrations() that there's a new workers[] entry.
            newCond.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Shutdown is an RPC method that shuts down the Master's RPC server.
     */
    @Override
    public void shutdown() {
        debug("Shutdown: registration server");
        if (isExported && rpc != null) {
            rpc.unExport();
            isExported = false;
        }
    }

    /**
     * helper function that sends information about all existing
     * and newly registered workers to channel ch. schedule()
     * reads ch to learn about workers.
     *
     * @param ch channel
     */
    public void forwardRegistrations(Channel<String> ch) {
        int i = 0;
        while (true) {
            try {
                lock.lock();

                if (workers.size() > i) {
                    // there's a worker that we haven't told schedule() about.
                    String w = workers.get(i);
                    Thread t = new Thread(() -> {
                        try {
                            ch.write(w);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                    t.setDaemon(true); // auto-shutdown
                    t.start();
                    i++;
                } else {
                    // wait for Register() to add an entry to workers[]
                    // in response to an RPC from a new worker.
                    try {
                        newCond.await();
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Distributed schedules map and reduce tasks on workers that register with the
     * master over RPC.
     *
     * the params are similar to {@link Master#sequential(String, String[], int, MapFunc, ReduceFunc)}
     * except that {@code master} is the master's address
     *
     * @param jobName
     * @param files
     * @param nReduce
     * @param master the master's address
     * @return master instance
     */
    public static Master distributed(String jobName, String[] files, int nReduce, String master) {
        Master mr = new Master(master);
        mr.startRPCServer();
        Thread run = new Thread(() -> {
            Channel<String> ch = new Channel<>();
            Thread t = new Thread(() -> {
                mr.forwardRegistrations(ch);
            });
            t.setDaemon(true);
            t.start();

            mr.run(jobName, files, nReduce, jobPhase -> {
                Scheduler.schedule(mr.jobName, mr.files, mr.nReduce, jobPhase, ch);
                return null;
            }, aVoid -> {
                mr.stopRPCServer();
                mr.stats = mr.killWorkers();
                return null;
            });
        });
        run.setDaemon(true);
        run.start();
        return mr;
    }

    /**
     * run executes a mapreduce job on the given number of mappers and reducers.
     * <p>
     * First, it divides up the input file among the given number of mappers, and
     * schedules each task on workers as they become available. Each map task bins
     * its output in a number of bins equal to the given number of reduce tasks.
     * Once all the mappers have finished, workers are assigned reduce tasks.
     * <p>
     * When all tasks have been completed, the reducer outputs are merged,
     * statistics are collected, and the master is shut down.
     * <p>
     * Note that this implementation assumes a shared file system.
     *
     * @param jobName
     * @param files
     * @param nReduce
     * @param schedule schedule function called to schedule map and reduce tasks
     * @param finish finish function called when all tasks are done
     */
    public void run(String jobName, String[] files, int nReduce, Func<Void, JobPhase> schedule, Func<Void, Void> finish) {
        this.jobName = jobName;
        this.files = files;
        this.nReduce = nReduce;

        System.out.println(String.format("%s: Starting Map/Reduce task %s", this.address, this.jobName));

        schedule.func(JobPhase.MAP_PHASE);
        schedule.func(JobPhase.REDUCE_PHASE);
        finish.func(null);
        merge();

        System.out.println(String.format("%s: Map/Reduce task completed", this.address));
        try {
            this.doneChannel.write(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * mWait blocks until the currently scheduled work has completed.
     * This happens when all tasks have scheduled and completed, the final output
     * have been computed, and all workers have been shut down.
     */
    public void mWait() {
        try {
            this.doneChannel.read(); // this will block the current thread
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void removeFile(String n) {
        File file = new File(n);
        if (file.exists() && !file.delete())
            System.err.println("CleanupFiles error");
    }

    public void cleanupFiles() {
        for (int i = 0; i < files.length; i++) {
            for (int j = 0; j < nReduce; j++) {
                removeFile(Utils.reduceName(jobName, i, j));
            }
        }
        for (int i = 0; i < nReduce; i++) {
            removeFile(Utils.mergeName(jobName, i));
        }
        removeFile("mrtmp." + jobName);
    }

    private void startRPCServer() {
        if (isExported)
            return;

        if (rpc == null) {
            // start RPC server
            ServerConfig serverConfig = new ServerConfig()
                    .setProtocol("bolt") // Set a protocol, which is bolt by default
                    .setPort(MASTER_PORT) // set a port, which is 12200 by default
                    .setDaemon(true); // daemon thread

            rpc = new ProviderConfig<MasterRpcService>()
                    .setInterfaceId(MasterRpcService.class.getName()) // Specify the interface
                    .setUniqueId(address)
                    .setRef(this) // Specify the implementation
                    .setServer(serverConfig); // Specify the server

            rpc.export();

        } else {
            rpc.export();
        }

        isExported = true;
    }

    /**
     * stopRPCServer stops the master RPC server.
     * This must be done through an RPC to avoid race conditions between the RPC
     * server thread and the current thread.
     */
    private void stopRPCServer() {
        try {
            Call.getMasterRpcService(address).shutdown();
            debug("cleanupRegistration: done");
        } catch (Exception e) {
            if (Utils.debugEnabled)
                e.printStackTrace();
            System.err.println(String.format("Cleanup: RPC %s error", address));
        }
    }

    private List<Integer> killWorkers() {
        try {
            lock.lock();
            List<Integer> nTasks = new ArrayList<>(workers.size());
            for (String w : workers) {
                debug(String.format("Master: shutdown worker %s", w));
                try {
                    int temp = Call.getWorkerRpcService(w).shutdown();
                    nTasks.add(temp);
                } catch (Exception e) {
                    System.err.println(String.format("Master: RPC %s shutdown error", w));
                }
            }
            return nTasks;
        } finally {
            lock.unlock();
        }
    }

    /**
     * merge combines the results of the many reduce jobs into a single output file
     * XXX use merge sort
     */
    private void merge() {
        debug("Merge phase");
        Map<String, Object> kvs = new LinkedHashMap<>();
        for (int i = 0; i < nReduce; i++) {
            String p = Utils.mergeName(jobName, i);
            System.out.println(String.format("Merge: read %s", p));

            try {
                // only suitable for small files
                JSONObject json = JSONObject.parseObject(new String(Files.readAllBytes(new File(p).toPath()), StandardCharsets.UTF_8),
                        Feature.OrderedField);
                kvs.putAll(json.getInnerMap());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        List<String> keyList = new ArrayList<>(kvs.keySet());
        Collections.sort(keyList);

        File file = new File("mrtmp." + jobName);
        // files are allowed to be overwritten
        try (BufferedWriter bw = Files.newBufferedWriter(file.toPath(),
                Charset.forName("UTF-8"))) {

            keyList.forEach(k -> {
                try {
                    bw.write(String.format("%s: %s\n", k, kvs.get(k)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
