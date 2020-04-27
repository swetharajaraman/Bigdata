package com.swetha;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TopKWords {
  private int k;
  private PriorityQueue<WordFrequency> priorityQueue;
  private Map<String, Long> wordFrequencyMap;

  public TopKWords(String file, int k) throws InterruptedException, ExecutionException {
    this.k = k;
    this.priorityQueue = new PriorityQueue(k);
    this.wordFrequencyMap = new ConcurrentHashMap<>();
    // System.out.println("Num cores " + Runtime.getRuntime().availableProcessors());
    ExecutorService executors = Executors.newFixedThreadPool(64);
    List<Callable<Long>> callables = new ArrayList<>();
    List<FileSplit> splits = FileSplit.getSplits(file, new File(file).length(), new File(file).length() / 1024);
    // System.out.println(splits);
    for(FileSplit split : splits) {
      Callable<Long> task = new ProcessFile(split, wordFrequencyMap);
      callables.add(task);
    }
    long start = System.currentTimeMillis();
    System.out.println("Total number of tasks " + callables.size());
    List<Future<Long>> futures = executors.invokeAll(callables);
    long totalBytes = 0;
    for(Future<Long> f : futures) {
      totalBytes += f.get();
    }
    System.out.println("Total bytes read " + totalBytes);
    System.out.println("Total time taken " + (System.currentTimeMillis() - start) + " ms");
    executors.shutdown();
  }

  public List<WordFrequency> topK() {
    processWordFrequencies();
    System.out.println("WordFrequencyMap keys size " + wordFrequencyMap.size());
    List<WordFrequency> result = new ArrayList<>();
    while (!priorityQueue.isEmpty()) {
      result.add(priorityQueue.poll());
    }
    return result;
  }

  public void processWordFrequencies() {
    for (Map.Entry<String, Long> entry : wordFrequencyMap.entrySet()) {
      WordFrequency wordFrequency = new WordFrequency(entry.getKey(), entry.getValue());
      addToTopKPriorityQueue(wordFrequency);
    }
  }

  private void addToTopKPriorityQueue(WordFrequency wordFrequency) {
    if (priorityQueue.size() < k) {
      priorityQueue.add(wordFrequency);
    } else if (priorityQueue.peek() != null && priorityQueue.peek().getFrequency() < wordFrequency.getFrequency()) {
      priorityQueue.poll();
      priorityQueue.add(wordFrequency);
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
