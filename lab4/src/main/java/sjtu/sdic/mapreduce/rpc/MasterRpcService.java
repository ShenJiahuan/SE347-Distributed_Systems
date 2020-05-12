package sjtu.sdic.mapreduce.rpc;

/**
 * These are all RPC methods of a Master
 * Created by Cachhe on 2019/4/21.
 */
public interface MasterRpcService {

    void register(String worker);

    void shutdown();
}
