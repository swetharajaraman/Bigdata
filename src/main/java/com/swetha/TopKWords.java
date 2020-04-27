package com.swetha;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Main class to compute TopK words from a text file.
 */
public class TopKWords {
  private int k;
  private PriorityQueue<WordFrequency> priorityQueue;
  private Map<String, Long> wordFrequencyMap;

  public TopKWords(String file, int k) throws InterruptedException, ExecutionException {
    this.k = k;
    this.priorityQueue = new PriorityQueue(k);
    this.wordFrequencyMap = new ConcurrentHashMap<>();
    ExecutorService executors = Executors.newFixedThreadPool(64);
    List<Callable<Long>> callables = new ArrayList<>();
    List<FileSplit> splits = FileSplit.getSplits(file, new File(file).length(), new File(file).length() / 32);
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
    System.out.println("Total time taken " + (System.currentTimeMillis() - start) + " ms");
    executors.shutdown();
  }

  /**
   * Return list of topK frequent words
   * @return
   */
  public List<String> topK() {
    processWordFrequencies();
    List<String> result = new ArrayList<>();
    while (!priorityQueue.isEmpty()) {
      result.add(priorityQueue.poll().getWord());
    }
    Collections.reverse(result);
    return result;
  }

  /**
   * Iterate through each entry in the wordFrequencyMap and add it to the topK priority queue
   */
  public void processWordFrequencies() {
    for (Map.Entry<String, Long> entry : wordFrequencyMap.entrySet()) {
      WordFrequency wordFrequency = new WordFrequency(entry.getKey(), entry.getValue());
      addToTopKPriorityQueue(wordFrequency);
    }
  }

  /**
   * Compare current word's frequency with PriorityQueue's peek frequency if greater remove PriorityQueue min element
   * and add the current word into the priority queue
   * @param wordFrequency
   */
  private void addToTopKPriorityQueue(WordFrequency wordFrequency) {
    if (priorityQueue.size() < k) {
      priorityQueue.add(wordFrequency);
    } else if (priorityQueue.peek() != null && priorityQueue.peek().getFrequency() < wordFrequency.getFrequency()) {
      priorityQueue.poll();
      priorityQueue.add(wordFrequency);
    }
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    TopKWords topk = new TopKWords("C:\\dataset-400MB.txt", 10);
    System.out.println(topk.topK());
  }

  private long toBytes(long sizeInMB) {
    return sizeInMB * 1024 * 1024;
  }
}
