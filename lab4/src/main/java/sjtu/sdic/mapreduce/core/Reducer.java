package sjtu.sdic.mapreduce.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Utils;

/** Created by Cachhe on 2019/4/19. */
public class Reducer {

  /**
   * doReduce manages one reduce task: it should read the intermediate files for the task, sort the
   * intermediate key/value pairs by key, call the user-defined reduce function {@code reduceF} for
   * each key, and write reduceF's output to disk.
   *
   * <p>You'll need to read one intermediate file from each map task; {@code reduceName(jobName, m,
   * reduceTask)} yields the file name from map task m.
   *
   * <p>Your {@code doMap()} encoded the key/value pairs in the intermediate files, so you will need
   * to decode them. If you used JSON, you can refer to related docs to know how to decode.
   *
   * <p>In the original paper, sorting is optional but helpful. Here you are also required to do
   * sorting. Lib is allowed.
   *
   * <p>{@code reduceF()} is the application's reduce function. You should call it once per distinct
   * key, with a slice of all the values for that key. {@code reduceF()} returns the reduced value
   * for that key.
   *
   * <p>You should write the reduce output as JSON encoded KeyValue objects to the file named
   * outFile. We require you to use JSON because that is what the merger than combines the output
   * from all the reduce tasks expects. There is nothing special about JSON -- it is just the
   * marshalling format we chose to use.
   *
   * <p>Your code here (Part I).
   *
   * @param jobName the name of the whole MapReduce job
   * @param reduceTask which reduce task this is
   * @param outFile write the output here
   * @param nMap the number of map tasks that were run ("M" in the paper)
   * @param reduceF user-defined reduce function
   */
  public static void doReduce(
      String jobName, int reduceTask, String outFile, int nMap, ReduceFunc reduceF) {
    final List<KeyValue> kvs = new ArrayList<>();
    for (int i = 0; i < nMap; i++) {
      final String inFile = Utils.reduceName(jobName, i, reduceTask);
      String content;

      try {
        content = new String(Files.readAllBytes(Paths.get(inFile)));
      } catch (IOException ex) {
        ex.printStackTrace();
        return;
      }

      final JSONArray jsonArray = JSONArray.parseArray(content);
      jsonArray.forEach(o -> kvs.add(((JSONObject) o).toJavaObject(KeyValue.class)));
    }
    kvs.sort(Comparator.comparing(o -> o.key));

    final JSONObject reduceResult = new JSONObject();
    List<String> values = null;
    String key = null;
    for (KeyValue kv : kvs) {
      if (!kv.key.equals(key)) {
        if (key != null) {
          final String res = reduceF.reduce(key, values.toArray(new String[values.size()]));
          reduceResult.put(key, res);
        }
        key = kv.key;
        values = new ArrayList<>();
      }
      values.add(kv.value);
    }
    final String res = reduceF.reduce(key, values.toArray(new String[values.size()]));
    reduceResult.put(key, res);

    try {
      Files.write(Paths.get(outFile), reduceResult.toJSONString().getBytes());
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }
}
