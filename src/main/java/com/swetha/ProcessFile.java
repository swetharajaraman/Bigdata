package com.swetha;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
    long lines = 0;
    RandomAccessFile randomAccess = null;
    BufferedReader reader = null;
    try {
      randomAccess = new RandomAccessFile(split.file, "r");
      if(skipLine) {
        randomAccess.seek(start--);
        // System.out.println("Skipping first line");
        line = randomAccess.readLine();
        start += line != null ? line.length() : 0;
      }
      pos = start;
      reader = new BufferedReader(new FileReader(randomAccess.getFD()));
      while ((line = reader.readLine()) != null && pos <= end) {
        String[] words = line.split("\\s+");
        for (String word : words) {
          addToWordFrequencyMap(word);
        }
        pos += line.length();
        count += line.length();
        lines++;
      }
      System.out.println(randomAccess.getFilePointer());
    } catch(FileNotFoundException fne) {
      System.err.println(fne.getMessage());
    } catch(IOException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (randomAccess != null) {
          randomAccess.close();
        }

        if(reader != null) {
          reader.close();
        }
      } catch(IOException e) {
        System.err.println(e.getMessage());
      }
    }
    System.out.println(lines);
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
