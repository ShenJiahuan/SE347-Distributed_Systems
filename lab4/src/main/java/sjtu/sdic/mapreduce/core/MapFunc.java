package sjtu.sdic.mapreduce.core;

import sjtu.sdic.mapreduce.common.KeyValue;

import java.util.List;

/**
 * Created by Cachhe on 2019/4/19.
 */
public interface MapFunc {

    /**
     *
     * @param file the path, if in the same dir, then it can be the filename
     * @param contents the contents of the file, must be coded in UTF-8
     * @return a list of k-vs
     */
    List<KeyValue> map(String file, String contents);
}
