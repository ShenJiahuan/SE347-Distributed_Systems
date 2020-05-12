package sjtu.sdic.mapreduce.core;

import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.request.SofaRequest;
import com.alipay.sofa.rpc.core.response.SofaResponse;
import com.alipay.sofa.rpc.filter.Filter;
import com.alipay.sofa.rpc.filter.FilterInvoker;
import sjtu.sdic.mapreduce.common.DoTaskArgs;
import sjtu.sdic.mapreduce.common.Parallelism;
import sjtu.sdic.mapreduce.common.Utils;
import sjtu.sdic.mapreduce.rpc.Call;
import sjtu.sdic.mapreduce.rpc.WorkerRpcService;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Worker holds the state for a server waiting for DoTask or Shutdown RPCs
 *
 * Created by Cachhe on 2019/4/22.
 */
public class Worker implements WorkerRpcService {
    public static final int WORKER_PORT = 13300;

    private Lock lock;
    public String name;
    private MapFunc mapF;
    private ReduceFunc reduceF;
    private volatile int nRPC; // quit after this many RPCs; protected by mutex
    private Condition rpcCond;

    private int nTasks; // total tasks executed; protected by mutex
    private int concurrent; // number of parallel DoTasks in this worker; mutex

    private ProviderConfig<WorkerRpcService> providerConfig; // handle of RPC service

    private Parallelism parallelism;

    private Worker() {
        lock = new ReentrantLock();
        rpcCond = lock.newCondition();
    }


    /**
     * Shutdown is called by the master when all work has been completed.
     * We should respond with the number of tasks we have processed.
     *
     */
    @Override
    public int shutdown() {
        Utils.debug(String.format("Shutdown %s", name));
        int temp;
        try {
            lock.lock();
            temp = nTasks;
            nRPC = 0;
            rpcCond.signal();
        } finally {
            lock.unlock();
        }
        return temp;
    }

    /**
     * DoTask is called by the master when a new task is being scheduled on this
     * worker.
     *
     * @param arg
     */
    @Override
    public void doTask(DoTaskArgs arg) {
        System.out.println(String.format("%s: given %s task #%d on file %s (nios: %d)",
                name, arg.phase, arg.taskNum, arg.file, arg.numOtherPhase));

        int nc;
        try {
            lock.lock();

            nTasks += 1;
            concurrent += 1;

            nc = concurrent;
        } finally {
            lock.unlock();
        }

        if (nc > 1) {
            // schedule() should never issue more than one RPC at a
            // time to a given worker.
            throw new RuntimeException("Worker.doTask: more than one DoTask sent concurrently to a single worker");
        }

        boolean pause = false;
        if (parallelism != null) {
            try {
                parallelism.lock.lock();
                parallelism.now += 1;
                if (parallelism.now > parallelism.max)
                    parallelism.max = parallelism.now;
                if (parallelism.max < 2)
                    pause = true;
            } finally {
                parallelism.lock.unlock();
            }
        }

        if (pause) {
            // give other workers a chance to prove that
            // they are executing in parallel.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        switch (arg.phase) {
            case MAP_PHASE:
                Mapper.doMap(arg.jobName, arg.taskNum, arg.file, arg.numOtherPhase, mapF);
                break;
            case REDUCE_PHASE:
                Reducer.doReduce(arg.jobName, arg.taskNum, Utils.mergeName(arg.jobName, arg.taskNum), arg.numOtherPhase, reduceF);
                break;
        }

        try {
            lock.lock();
            concurrent -= 1;
        } finally {
            lock.unlock();
        }

        if (parallelism != null) {
            try {
                parallelism.lock.lock();
                parallelism.now -= 1;
            } finally {
                parallelism.lock.unlock();
            }
        }

        System.out.println(String.format("%s: %s task #%d done", name, arg.phase, arg.taskNum));
    }

    /**
     * RunWorker sets up a connection with the master, registers its address, and
     * waits for tasks to be scheduled.
     *
     * @param master master address
     * @param me worker address
     * @param mapF user-defined map function
     * @param reduceF user-defined reduce function
     * @param nRPC maximum service times, counting by each request
     * @param parallelism test whether runs in parallel, nullable
     */
    public static void runWorker(String master, String me, MapFunc mapF,
                          ReduceFunc reduceF, int nRPC, Parallelism parallelism) {
        Utils.debug(String.format("RunWorker %s", me));

        Worker wk = new Worker();
        wk.name = me;
        wk.mapF = mapF;
        wk.reduceF = reduceF;
        wk.nRPC = nRPC;
        wk.parallelism = parallelism;

        new Thread(() -> wk.run(master)).start();
    }

    /**
     * Tell the master we exist and ready to work
     * @param master master address
     */
    public void register(String master) {
        try {
            Call.getMasterRpcService(master).register(name);
        } catch (Exception e) {
            if (Utils.debugEnabled)
                e.printStackTrace();
            System.err.println(String.format("Register: RPC %s register error", name));
            shutdown();
        }
    }

    /**
     * generate a valid RPC service port according to the
     * worker address (name)
     *
     * Note: there may be port CONFLICTS with little probability
     * if u encounter with this, try to rename the worker
     *
     * @param worker ip address of worker
     * @return the port
     */
    public static int getPort(String worker) {
        int port = (worker.hashCode() & Integer.MAX_VALUE) % 65535;
        return port < WORKER_PORT ? port + WORKER_PORT : port;
    }

    /**
     * called when a RPC request comes
     */
    private static class RPCFilter extends Filter {
        private Worker worker;

        RPCFilter(Worker worker) {
            this.worker = worker;
        }

        @Override
        public SofaResponse invoke(FilterInvoker invoker, SofaRequest request) throws SofaRpcException {
            try {
                worker.lock.lock();
                if (worker.nRPC == 0) {
                    throw new RuntimeException("Worker is already offline");
                }
                worker.nRPC -= 1;
                worker.rpcCond.signal();
            } finally {
                worker.lock.unlock();
            }
            return invoker.invoke(request);
        }
    }

    private void run(String master) {
        ServerConfig serverConfig = new ServerConfig()
                .setProtocol("bolt") // Set a protocol, which is bolt by default
                .setPort(getPort(name)) // set a port, which is 13300 by default
                .setDaemon(true); // daemon thread

        providerConfig = new ProviderConfig<WorkerRpcService>()
                .setInterfaceId(WorkerRpcService.class.getName()) // Specify the interface
                .setRef(this) // Specify the implementation
                .setUniqueId(name)
                .setFilterRef(Collections.singletonList(new RPCFilter(this)))
                .setServer(serverConfig); // Specify the server

        providerConfig.export(); // Publish service


        register(master);

        // DON'T MODIFY CODE BELOW
        while (true) {
            try {
                lock.lock();
                if (nRPC == 0) {
                    providerConfig.unExport();
                    break;
                }
                try {
                    rpcCond.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } finally {
                lock.unlock();
            }
        }

        Utils.debug(String.format("RunWorker %s exit", name));
    }
}
