# Lab 4: Map Reduce

## Part 1:  Map/Reduce input and output

- `doMap()` in `Mapper.java`:
  1. Read content of `inFile`.
  2. Call `mapF` to get mapper's result, a `List<KeyValue>`.
  3. Create an array of `JSONArray`, with `nReduce` as length.
  4. For each `KeyValue` pair `kv` returned by mapper, calculate `hashCode(kv.key) % nReduce` as index, and add `kv` into corresponding `JSONArray`.
  5. These `JSONArray`s are the intermediate files for Reducer to read, so write each file with name calculated by `Utils.reduceName`.
- `doReduce()` in `Reducer.java`:
  1. As we will create `nMap * nReduce` intermediate files, each reducer will read `nMap` files, and add all the `KeyValue` pairs in these files to a list.
  2. Sort these `KeyValue` pairs by key.
  3. Call `reduceF` for each distinct key, and values are all the values in the `KeyValue` pairs of the same key.
  4. Add pair of `key` and `reduceF`'s result to `JSONObject`, which will be the output of this Reducer.

## Part II: Single-worker word count

- `mapFunc()` in `WordCount.java`:
  1. Use regular expression ` [a-zA-Z0-9]+` to get all the words in file's content.
  2. For each word, create a `KeyValue` pair `(word, "1")` and add it to the result list.
  3. Return the result list.
- `reduceFunc()` in `WordCount.java`:
  1. Parameter `String[] values` contains occurance of the `key`, each `String` is a number in string format.
  2. What we only need to do is to sum these values up, and convert it back to `String`.

## Part III: Distributing MapReduce tasks

- `schedule()` in `Schedule.java`:

  1. There are in total n tasks, each should be delivered to a worker, so we create a `ConcurrentLinkedDeque`, which contains all the works that haven't been finished.
  2. While this deque is not empty, we will assign workers to complete these tasks.
  3. Avaiable worker can be get from `registerChan.read()`.
  4. For each worker, we create a thread, which:
     1. Get worker by name: `Call.getWorkerRpcService(finalWorker)`.
     2. Call `WorkerRpcService.doTask` to finish the work.
     3. Add this worker back to `registerChan` after it has finished the work.

  5. Start the created thread, and add this thread to `List<Thread>`.
  6. When the deque is empty, we call `thread.join()` for each created thread, waiting for them to finish their work before `schedule()` return.

## Part IV: Handling worker failures

- `schedule()` in `Schedule.java`:
  1. We should add additional logic to `schedule()` to handle worker failure.
  2. We can use try-cache to notice worker failure during `getWorkerRpcService()` or `doTask()`.
  3. If it failed, add back this taskNum to the `workDeque`, in order to let another worker redo it, and we do not add this failed worker back to `registerChan`.
  4. One consequence of this change is that: `workDeque` might be empty in some time, as each work has been assigned to a worker, but later when the worker failed, this work would be added back to `workDeque` so it will be not empty again. In order to handle this situation, we need to add another loop to check `workDeque` when all the worker threads has exited.

## Part V: Inverted index generation (optional, as bonus)

- `mapFunc()` in `InvertedIndex.java`:
  1. Logic is similar to `WordCount`, but in `InvertedIndex`, value of the `KeyValue` pair is not `"1"`, but the file containing the word.
- `reduceFunc()` in `InvertedIndex.java`:
  1. Parameter `String[] values` means all the files that contains word `key`.
  2. In order to create format `word: #documents documents,sorted,and,separated,by,commas`, reducer is responsible to create `#documents documents,sorted,and,separated,by,commas`.
  3. We first deduplicate the values and sort them by lexicographical order, and then calculate length of the resulting array, and join elements of the array by `','`.