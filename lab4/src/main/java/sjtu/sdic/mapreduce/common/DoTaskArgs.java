package sjtu.sdic.mapreduce.common;

/**
 * DoTaskArgs holds the arguments that are passed to a worker when
 * a job is scheduled on it.
 *
 * Created by Cachhe on 2019/4/23.
 */
public class DoTaskArgs {
    public String jobName;
    public String file; // only for map, the input file
    public JobPhase phase; // are we in mapPhase or reducePhase?
    public int taskNum; // this task's index in the current phase

    // numOtherPhase is the total number of tasks in other phase; mappers
    // need this to compute the number of output bins, and reducers needs
    // this to know how many input files to collect.
    public int numOtherPhase;

    public DoTaskArgs(String jobName, String file, JobPhase phase, int taskNum, int numOtherPhase) {
        this.jobName = jobName;
        this.file = file;
        this.phase = phase;
        this.taskNum = taskNum;
        this.numOtherPhase = numOtherPhase;
    }
}
