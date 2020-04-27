package com.swetha;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 *  ProcessFile is a Callable task which reads file line by line, splits lines in to words
 *  and adds it to the wordFrequencyMap to maintain word and its frequencies.
 */
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

  /**
   *  Here we directly seek to the start of the file by randomly seeking to the start offset using RandomFile API.
   *  Note: Skip the first line if the start offset is > 0 as it would be already processed in the previous split task
   */
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

  /**
   * Add (word, 1) if word is not already present in the wordFrequencyMap if not increment the frequency by 1
   * and add it to the wordFrequencyMap
   *
   * @param word
   */
  private void addToWordFrequencyMap(String word) {
    wordFrequencyMap.putIfAbsent(word, 1L);
    wordFrequencyMap.computeIfPresent(word, (key, value) -> value + 1);
  }

  @Override
  public Long call() {
    return processFile();
  }
}
