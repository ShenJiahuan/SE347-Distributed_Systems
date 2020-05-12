package sjtu.sdic.mapreduce.common;

/**
 * A function-style interface supporting ONE param
 * if there's no param or no returned value, use {@link Void}
 *
 * Created by Cachhe on 2019/4/19.
 */
public interface Func<R, T> {

    /**
     *
     * @param t argument
     * @return result of this function
     */
    R func(T t);
}
