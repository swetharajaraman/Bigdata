package com.swetha;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ProcessFile implements Callable<Map<String, Long>> {
  private FileSplit split;
  private Map<String, Long> frequencyMap;
  private boolean skipLine;
  private long start;
  private long end;
  private long pos;

  public ProcessFile(FileSplit split) {
    this.split = split;
    this.frequencyMap = new HashMap<>(1000000);
    this.start = split.start;
    this.skipLine = (start != 0);
    this.end = split.start + split.length;
  }

  private long processFile() {
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
    System.out.println(count);
    return count;
  }

  private void addToWordFrequencyMap(String word) {
      long count = frequencyMap.getOrDefault(word, 0L) + 1;
      frequencyMap.put(word, count);
  }

  @Override
  public Map<String, Long> call() {
    // System.out.println("Processing file");
    processFile();
    return frequencyMap;
  }
}
