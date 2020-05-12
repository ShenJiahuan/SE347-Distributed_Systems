package sjtu.sdic.mapreduce.rpc;

import sjtu.sdic.mapreduce.common.DoTaskArgs;

/**
 * These are all RPC methods of a Worker
 * Created by Cachhe on 2019/4/22.
 */
public interface WorkerRpcService {

    int shutdown();

    void doTask(DoTaskArgs arg);
}
