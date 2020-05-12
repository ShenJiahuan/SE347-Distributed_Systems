package sjtu.sdic.mapreduce.rpc;

import com.alipay.sofa.rpc.config.ConsumerConfig;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;



/**
 * Created by Cachhe on 2019/4/22.
 */
public class Call {

    /**
     * get RPC service of the master with specified address
     *
     * @param address master address
     * @return the master's RPC service, null if no service runs at given address
     */
    public static MasterRpcService getMasterRpcService(String address) {
            ConsumerConfig<MasterRpcService> consumerConfig = new ConsumerConfig<MasterRpcService>()
                    .setInterfaceId(MasterRpcService.class.getName()) // Specify the interface
                    .setUniqueId(address)
                    .setProtocol("bolt") // Specify the protocol
                    .setDirectUrl("bolt://127.0.0.1:" + Master.MASTER_PORT)
                    .setRepeatedReferLimit(-1);
       return consumerConfig.refer();
    }

    /**
     * get RPC service of the worker with specified address
     *
     * @param address worker address
     * @return the worker's RPC service, null if no service runs at given address
     */
    public static WorkerRpcService getWorkerRpcService(String address) {
            ConsumerConfig<WorkerRpcService> consumerConfig = new ConsumerConfig<WorkerRpcService>()
                    .setInterfaceId(WorkerRpcService.class.getName()) // Specify the interface
                    .setUniqueId(address)
                    .setProtocol("bolt") // Specify the protocol
                    .setDirectUrl("bolt://127.0.0.1:" + Worker.getPort(address))
                    .setRepeatedReferLimit(-1);
        return consumerConfig.refer();

    }
}
