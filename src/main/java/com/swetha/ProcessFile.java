package com.swetha;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ProcessFile implements Callable<Long> {
  private FileSplit split;
  private final Map<String, Long> wordFrequencyMap;
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
    long numLines = 0;
    String line;
    long count = 0;
    RandomAccessFile randomAccess = null;
    BufferedReader reader = null;
    try {
      randomAccess = new RandomAccessFile(split.file, "r");
      if(skipLine) {
        randomAccess.seek(start--);
        // Skipping first line
        reader = new BufferedReader(new FileReader(randomAccess.getFD()));
        line = reader.readLine();
        start += line != null ? line.length():0;
      }
      long pos = start;
      long startPosition = pos;
      reader = new BufferedReader(new FileReader(randomAccess.getFD()));
      while ((line = reader.readLine()) != null && pos <= end) {
        String[] words = line.split("\\s+");
        for (String word : words) {
          if(word != null && !word.isEmpty()) {
            addToWordFrequencyMap(word);
          }
        }
        pos += line.length();
        count += line.getBytes().length;
        numLines++;
      }

      // System.out.println("Num lines " + numLines);
      // System.out.println("File Split start = " + split.start + " startPosition = " + startPosition + " end = " + end + " last file position " + randomAccess.getFilePointer());
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
    wordFrequencyMap.putIfAbsent(word, 1L);
    wordFrequencyMap.computeIfPresent(word, (key, value) -> value + 1);
  }

  private void writeToFile(Map<String, Long> map) {
    File writeFile = new File("FileSplit-" + this.split.id);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(writeFile))) {
      for(Map.Entry<String, Long> entry : map.entrySet()) {
        writer.write(entry.getKey() + "-" + entry.getValue());
        writer.write("\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Long call() {
    // System.out.println("Processing file");
    return processFile();
  }

  public static void main(String[] args) {
    FileSplit split = new FileSplit(0,8506981461L, 66984105L, "/Users/vsowrira/Downloads/dataset-8GB.txt");
    ProcessFile process = new ProcessFile(split, new HashMap<>());
    process.processFile();
  }
}
