package com.swetha;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;


public class ProcessFile implements Callable<Long> {
  private FileSplit split;
  private Map<String, Long> wordFrequencyMap;
  private Map<String, Long> localMap;
  private boolean skipLine;
  private long start;
  private long end;
  private long pos;

  public ProcessFile(FileSplit split, Map<String, Long> wordFrequencyMap) {
    this.split = split;
    this.localMap = new ConcurrentHashMap<>();
    this.wordFrequencyMap = wordFrequencyMap;
    this.start = split.start;
    this.skipLine = (start != 0);
    this.end = split.start + split.length;
  }

  public long processFile() {
    String line;
    long count = 0;
    try(RandomAccessFile randomAccess = new RandomAccessFile(split.file, "r")) {
      if(skipLine) {
        randomAccess.seek(start--);
        // System.out.println("Skipping first line");
        start += randomAccess.readLine().length();
      }
      pos = start;
      while ((line = randomAccess.readLine()) != null && pos <= end) {
        String[] words = line.split("\\s+");
        for (String word : words) {
          addToWordFrequencyMap(word);
        }
        pos += line.length();
        count += line.length();
      }
    } catch(FileNotFoundException fne) {
      System.err.println(fne.getMessage());
    } catch(IOException e) {
      System.err.println(e.getMessage());
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
