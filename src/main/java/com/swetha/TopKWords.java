package com.swetha;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.*;

public class TopKWords {
  private int k;
  private PriorityQueue<WordFrequency> priorityQueue;
  private String file;
  private ExecutorService executors;

  public TopKWords(String file, int k) {
    this.k = k;
    this.priorityQueue = new PriorityQueue(k);
    this.file = file;
    System.out.println("Num cores " + Runtime.getRuntime().availableProcessors());
    executors = Executors.newFixedThreadPool(32);
  }

  private List<FileSplit> getSplits(String file, long fileLength, long splitSize) {
    long start = 0;
    List<FileSplit> splits = new ArrayList<>();
    while(fileLength > 0) {
      FileSplit split = new FileSplit(start, splitSize, file);
      splits.add(split);
      start += splitSize + 1;
      fileLength -= splitSize;
    }
    return splits;
  }

  public List<WordFrequency> topK() throws ExecutionException, InterruptedException {
    long start = System.currentTimeMillis();
    List<Callable<Map<String, Long>>> callables = new ArrayList<>();
    List<FileSplit> splits = getSplits(file, new File(file).length(), toBytes(16));
    System.out.println(splits);
    for(FileSplit split : splits) {
      Callable task = new ProcessFile(split);
      callables.add(task);
    }
    System.out.println("Total number of tasks " + callables.size());
    List<Future<Map<String, Long>>> futures = executors.invokeAll(callables);
    awaitTerminationAfterShutdown(executors);

    // Merge hashmaps into one single hashmap
    Map<String, Long> mergeMap = new HashMap<>();
    long totalBytes = 0;
    for(Future<Map<String, Long>> future : futures) {
      for(Map.Entry<String, Long> entry : future.get().entrySet()) {
        if(mergeMap.containsKey(entry.getKey())) {
          long value = mergeMap.get(entry.getKey()) + entry.getValue();
          mergeMap.put(entry.getKey(), value);
        } else {
          mergeMap.put(entry.getKey(), entry.getValue());
        }
      }
    }
    System.out.println("Total bytes read " + totalBytes);
    System.out.println("Total time taken " + (System.currentTimeMillis() - start) / 1000 + " secs");
    processWordFrequencies(mergeMap);
    System.out.println("WordFrequencyMap keys size " + mergeMap.size());
    List<WordFrequency> result = new ArrayList<>();
    while (!priorityQueue.isEmpty()) {
      result.add(priorityQueue.poll());
    }

    return result;
  }

  private void processWordFrequencies(Map<String, Long> wordFrequencyMap) {
    for (Map.Entry<String, Long> entry : wordFrequencyMap.entrySet()) {
      WordFrequency wordFrequency = new WordFrequency(entry.getKey(), entry.getValue());
      addToTopKPriorityQueue(wordFrequency);
    }
  }

  private void addToTopKPriorityQueue(WordFrequency wordFrequency) {
    if (priorityQueue.size() < k) {
      priorityQueue.add(wordFrequency);
    } else if (priorityQueue.peek().getFrequency() < wordFrequency.getFrequency()) {
      priorityQueue.poll();
      priorityQueue.add(wordFrequency);
    }
  }

  private void awaitTerminationAfterShutdown(ExecutorService threadPool) {
    threadPool.shutdown();
    try {
      if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
        threadPool.shutdownNow();
      }
    } catch (InterruptedException ex) {
      threadPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    String file = args[0];
    TopKWords topk = new TopKWords(file, 10);
    System.out.println(topk.topK());
  }

  private long toBytes(long sizeInMB) {
    return sizeInMB * 1024 * 1024;
  }
}
