package sjtu.sdic.mapreduce.core;

import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import sjtu.sdic.mapreduce.common.Channel;
import sjtu.sdic.mapreduce.common.DoTaskArgs;
import sjtu.sdic.mapreduce.common.JobPhase;
import sjtu.sdic.mapreduce.rpc.Call;

/** Created by Cachhe on 2019/4/22. */
public class Scheduler {

  /**
   * schedule() starts and waits for all tasks in the given phase (mapPhase or reducePhase). the
   * mapFiles argument holds the names of the files that are the inputs to the map phase, one per
   * map task. nReduce is the number of reduce tasks. the registerChan argument yields a stream of
   * registered workers; each item is the worker's RPC address, suitable for passing to {@link
   * Call}. registerChan will yield all existing registered workers (if any) and new ones as they
   * register.
   *
   * @param jobName job name
   * @param mapFiles files' name (if in same dir, it's also the files' path)
   * @param nReduce the number of reduce task that will be run ("R" in the paper)
   * @param phase MAP or REDUCE
   * @param registerChan register info channel
   */
  public static void schedule(
      String jobName,
      String[] mapFiles,
      int nReduce,
      JobPhase phase,
      Channel<String> registerChan) {
    int nTasks = -1; // number of map or reduce tasks
    int nOther = -1; // number of inputs (for reduce) or outputs (for map)
    switch (phase) {
      case MAP_PHASE:
        nTasks = mapFiles.length;
        nOther = nReduce;
        break;
      case REDUCE_PHASE:
        nTasks = nReduce;
        nOther = mapFiles.length;
        break;
    }

    System.out.println(String.format("Schedule: %d %s tasks (%d I/Os)", nTasks, phase, nOther));

    /**
     * // All ntasks tasks have to be scheduled on workers. Once all tasks // have completed
     * successfully, schedule() should return. // // Your code here (Part III, Part IV). //
     */
    Deque<Integer> workDeque = new ConcurrentLinkedDeque<>();
    for (int i = 0; i < nTasks; i++) {
      workDeque.add(i);
    }

    while (!workDeque.isEmpty()) {
      List<Thread> threads = new ArrayList<>();
      while (!workDeque.isEmpty()) {
        String worker;
        try {
          worker = registerChan.read();
        } catch (InterruptedException ex) {
          ex.printStackTrace();
          return;
        }

        final String finalWorker = worker;
        final int taskNum = workDeque.pop();
        final int finalNOther = nOther;
        Thread t =
            new Thread(
                () -> {
                  System.out.println("worker begin");
                  final String mapFile = phase == JobPhase.MAP_PHASE ? mapFiles[taskNum] : null;
                  try {
                    Call.getWorkerRpcService(finalWorker)
                        .doTask(new DoTaskArgs(jobName, mapFile, phase, taskNum, finalNOther));
                  } catch (SofaRpcException ex) {
                    workDeque.add(taskNum);
                    return; // we do not add this failed worker back to registerChan
                  }
                  try {
                    registerChan.write(finalWorker);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                });
        t.start();
        threads.add(t);
      }

      threads.forEach(
          t -> {
            try {
              t.join();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          });
    }

    System.out.println(String.format("Schedule: %s done", phase));
  }
}
