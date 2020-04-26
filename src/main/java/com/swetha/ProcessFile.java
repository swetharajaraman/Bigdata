package com.swetha;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.Callable;

public class ProcessFile implements Callable<Long> {
  private FileSplit split;
  private Map<String, Long> wordFrequencyMap;
  private boolean skipLine;
  private long start;
  private long end;

  public ProcessFile(FileSplit split, Map<String, Long> wordFrequencyMap) {
    this.split = split;
    this.wordFrequencyMap = wordFrequencyMap;
    this.start = split.start;
    this.skipLine = (start != 0);
    this.end = split.start + split.length;
  }

  public long processFile() {
    String line;
    long count = 0;
    RandomAccessFile randomAccess = null;
    BufferedReader reader = null;
    try {
      randomAccess = new RandomAccessFile(split.file, "r");
      if(skipLine) {
        randomAccess.seek(start--);
        // System.out.println("Skipping first line");
        start += randomAccess.readLine().length();
      }
      long pos = start;
      reader = new BufferedReader(new FileReader(randomAccess.getFD()));
      while ((line = reader.readLine()) != null && pos <= end) {
        String[] words = line.split("\\s+");
        for (String word : words) {
          addToWordFrequencyMap(word);
        }
        pos += line.length();
        count += line.getBytes().length;
      }
    } catch(IOException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (randomAccess != null) {
          randomAccess.close();
        }

        if (reader != null) {
          reader.close();
        }
      } catch(IOException e) {
        System.err.println(e.getMessage());
      }
    }
    return count;
  }

  private void addToWordFrequencyMap(String word) {
    synchronized (wordFrequencyMap) {
      long count = wordFrequencyMap.getOrDefault(word, 0L) + 1;
      wordFrequencyMap.put(word, count);
    }
  }

  @Override
  public Long call() {
    // System.out.println("Processing file");
    return processFile();
  }
}
