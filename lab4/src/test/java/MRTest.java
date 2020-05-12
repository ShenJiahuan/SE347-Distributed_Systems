import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Parallelism;
import sjtu.sdic.mapreduce.common.Utils;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Cachhe on 2019/4/19.
 */
public class MRTest {

    public static final int N_NUMBER = 100000;
    public static final int N_Map    = 20;
    public static final int N_Reduce = 10;

    // Split in words
    public List<KeyValue> mapFunc(String file, String value) {
        Utils.debug(value);

        List<KeyValue> keyValues = new ArrayList<>();

        String[] temp = value.split("\\s+");
        for (String s : temp) {
            keyValues.add(new KeyValue(s, ""));
        }
        return keyValues;
    }

    // Just return key
    public String reduceFunc(String key, String[] values) {
        for (String s : values) {
            Utils.debug(String.format("Reduce %s %s", key, s));
        }
        return "";
    }

    public String[] makeInputs(int num) {
        String[] names = new String[num];
        int i = 0;
        for (int k = 0; k < num; k++) {
            names[k] = String.format("824-mrinput-%d.txt", k);
            File file = new File(names[k]);

            try (BufferedWriter bw = Files.newBufferedWriter(file.toPath(),
                    Charset.forName("UTF-8"),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){

                while (i < (k + 1) * (N_NUMBER / num)) {
                    bw.write(String.format("%d\n", i));
                    i++;
                }
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return names;
    }

    /**
     * Checks input file agaist output file: each input number should show up
     * in the output file in string sorted order
     *
     * @param files
     */
    public void check(String[] files) {
        List<String> lines = new ArrayList<>();
        for (String f : files) {
            try {
                lines.addAll(Files.readAllLines(new File(f).toPath()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Collections.sort(lines);

        File output = new File("mrtmp.test");
        int i = 0;
        try (BufferedReader br = Files.newBufferedReader(output.toPath())){
            String text;
            while ((text = br.readLine()) != null) {
                int v1 = Integer.valueOf(text.split(":")[0]);
                int v2 = Integer.valueOf(lines.get(i).split(":")[0]);
                TestCase.assertFalse(String.format("line %d: %d != %d", i, v1, v2),v1 != v2);
                i++;
            }

            TestCase.assertFalse(String.format("Expected %d lines in output", N_NUMBER), i != N_NUMBER);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Workers report back how many RPCs they have processed in the Shutdown reply.
     * Check that they processed at least 1 DoTask RPC.
     * @param stats
     */
    public void checkWorker(List<Integer> stats) {
        for (int tasks : stats) {
            TestCase.assertFalse("A worker didn't do any work", tasks == 0);
        }
    }

    /**
     * this checks threads alive after map reduce task finish.
     *
     */
    public void checkThread() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Thread main = Thread.currentThread();

        ThreadGroup currentGroup =
                Thread.currentThread().getThreadGroup();
        int noThreads = currentGroup.activeCount();
        Thread[] lstThreads = new Thread[noThreads];
        currentGroup.enumerate(lstThreads);
        for (int i = 0; i < noThreads; i++){
            System.out.println("thread no：" + i + " = " + lstThreads[i].getName() + " "
            + lstThreads[i].getState().name());

            if (lstThreads[i].getId() != main.getId())
                TestCase.assertTrue(String.format("%s is not daemon", lstThreads[i].toString()), lstThreads[i].isDaemon() );
        }
    }


    public void cleanup(Master mr) {
        if (mr == null)
            return;

        mr.cleanupFiles();
        for (String f : mr.files) {
            mr.removeFile(f);
        }
    }

    public Master setup() {
        String[] files = makeInputs(N_Map);
        return Master.distributed("test", files, N_Reduce, "master");
    }

    @Test
    public void testSequentialSingle() {
        Master mr = null;
        try {
            mr = Master.sequential("test", makeInputs(1), 1, this::mapFunc, this::reduceFunc);
            mr.mWait();
            check(mr.files);
            checkWorker(mr.stats);
        } finally {
            cleanup(mr);
        }
    }

    @Test
    public void testSequentialMany() {
        Master mr = null;
        try {
            mr = Master.sequential("test", makeInputs(5), 3, this::mapFunc, this::reduceFunc);
            mr.mWait();
            check(mr.files);
            checkWorker(mr.stats);
        } finally {
            cleanup(mr);
        }
    }

    @Test
    public void testParallelBasic() {
        Master mr = null;
        try {
            mr = setup();
            AtomicInteger temp = new AtomicInteger(1);
            for (int i = 0; i < 2; i++) {
                Worker.runWorker(mr.address, "worker" + temp.getAndIncrement(),
                        this::mapFunc, this::reduceFunc, -1, null);
            }
            mr.mWait();
            check(mr.files);
            checkWorker(mr.stats);
            checkThread();
        } finally {
            cleanup(mr);
        }
    }

    @Test
    public void testParallelCheck() {
        Master mr = null;
        try {
            mr = setup();
            AtomicInteger temp = new AtomicInteger(1);
            Parallelism parallelism = new Parallelism();
            for (int i = 0; i < 2; i++) {
                Worker.runWorker(mr.address, "worker" + temp.getAndIncrement(),
                        this::mapFunc, this::reduceFunc, -1, parallelism);
            }

            mr.mWait();
            check(mr.files);
            checkWorker(mr.stats);

            try {
                parallelism.lock.lock();
                TestCase.assertFalse("workers didn't execute in parallel", parallelism.max < 2);
            } finally {
                parallelism.lock.unlock();
            }

            checkThread();
        } finally {
            cleanup(mr);
        }
    }

    @Test
    public void testOneFailure() {
        Master mr = null;
        try {
            mr = setup();
            Worker.runWorker(mr.address, "worker1",
                    this::mapFunc, this::reduceFunc, 5, null);
            Worker.runWorker(mr.address, "worker2",
                    this::mapFunc, this::reduceFunc, -1, null);

            mr.mWait();
            check(mr.files);
            checkWorker(mr.stats);
            checkThread();
        } finally {
            cleanup(mr);
        }
    }

    @Test
    public void testManyFailures() {
        Master mr = null;
        try {
            mr = setup();

            int i = 0;
            while (true) {
                Boolean done = mr.doneChannel.poll();
                if (done == null || !done) {
                    // Start 2 workers each sec. The workers fail after 10 tasks
                    Worker.runWorker(mr.address, ("worker" + i++), this::mapFunc, this::reduceFunc, 10, null);
                    Worker.runWorker(mr.address, ("worker" + i++), this::mapFunc, this::reduceFunc, 10, null);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    check(mr.files);

                    break;
                }
            }

            checkThread();
        } finally {
            cleanup(mr);
        }
    }
}
