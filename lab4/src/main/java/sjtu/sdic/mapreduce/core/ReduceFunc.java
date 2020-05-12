package sjtu.sdic.mapreduce.core;

/**
 * Created by Cachhe on 2019/4/19.
 */
public interface ReduceFunc {

    /**
     *
     * @param key the key
     * @param values the values with the same key
     * @return reducing result
     */
    String reduce(String key, String[] values);
}
