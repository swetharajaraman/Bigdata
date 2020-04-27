package com.swetha;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
  private List<FileSplit> splits;

  public TopKWords(String file, int k) throws InterruptedException, ExecutionException {
    this.k = k;
    this.priorityQueue = new PriorityQueue(k);
    this.wordFrequencyMap = new ConcurrentHashMap<>();
    // System.out.println("Num cores " + Runtime.getRuntime().availableProcessors());
    ExecutorService executors = Executors.newFixedThreadPool(64);
    List<Callable<Long>> callables = new ArrayList<>();
    splits = FileSplit.getSplits(file, new File(file).length(), new File(file).length() / 4);
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

  public List<WordFrequency> topKFromFile() throws FileNotFoundException {
    String file = "FileSplit-";
    boolean[] loadNext = new boolean[splits.size()];
    WordFrequency[] wordFrequencies = new WordFrequency[splits.size()];
    Arrays.fill(loadNext, true);
    BufferedReader[] readers = new BufferedReader[splits.size()];
    for(int i = 0;i < splits.size();i++) {
      readers[i] = new BufferedReader(new FileReader(new File(file + i)));
    }

    while(readNextLine(readers, loadNext, wordFrequencies)) {
      String word = lexicographicallySmallest(wordFrequencies);
      long count = 0;
      for(int i = 0;i < wordFrequencies.length;i++) {
        if(wordFrequencies[i].getWord() != null && wordFrequencies[i].getWord().equals(word)) {
          count += wordFrequencies[i].getFrequency();
          loadNext[i] = true;
        }
      }

      WordFrequency wordFrequency = new WordFrequency(word, count);
      addToTopKPriorityQueue(wordFrequency);
    }
    // System.out.println(priorityQueue);

    List<WordFrequency> topK = new ArrayList<>();
    while(!priorityQueue.isEmpty()) {
      topK.add(priorityQueue.poll());
    }
    return topK;
  }

  private boolean readNextLine(BufferedReader[] readers, boolean[] loadNext, WordFrequency[] wordFrequencies) {
    boolean isAvailable = false;
    for(int i = 0;i < loadNext.length;i++) {
      if(loadNext[i]) {
        try {
          String line = readers[i].readLine();
          if(line != null) {
            if(wordFrequencies[i] == null) {
              wordFrequencies[i] = new WordFrequency();
            }
            String[] pairs = line.split("\0");
            // System.out.println("Line = " + line + " word = " + pairs[0] + " frequency = " + pairs[1]);
            wordFrequencies[i].word = pairs[0];
            wordFrequencies[i].frequency = Long.parseLong(pairs[1]);
            isAvailable = true;
            loadNext[i] = false;
          } else {
            wordFrequencies[i].word = null;
            wordFrequencies[i].frequency = 0;
            loadNext[i] = false;
            readers[i].close();
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return isAvailable;
  }

  private String lexicographicallySmallest(WordFrequency[] wordFrequencies) {
      Arrays.sort(wordFrequencies, Comparator.comparing(WordFrequency::getWord));
      return wordFrequencies[0].getWord();
  }

  public List<String> topK() {
    processWordFrequencies();
    System.out.println("WordFrequencyMap keys size " + wordFrequencyMap.size());
    List<String> result = new ArrayList<>();
    while (!priorityQueue.isEmpty()) {
      result.add(priorityQueue.poll().getWord());
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

  public static void main(String[] args) throws ExecutionException, InterruptedException, FileNotFoundException {
    String file = args[0];
    TopKWords topk = new TopKWords(file, 10);
    System.out.println(topk.topKFromFile());
  }

  private long toBytes(long sizeInMB) {
    return sizeInMB * 1024 * 1024;
  }
}
