package com.swetha;

/**
 * WordFrequency is used to maintain word and its frequency and also compare 2 words by its frequencies.
 */

public class WordFrequency implements Comparable<WordFrequency> {
  private String word;
  private long frequency;

  public WordFrequency(String word, long frequency) {
    this.word = word;
    this.frequency = frequency;
  }

  @Override
  public int compareTo(WordFrequency o) {
    return (int) (this.frequency - o.frequency);
  }

  @Override
  public String toString() {
    return this.word + " - " + this.frequency;
  }

  public String getWord() {
    return word;
  }

  public long getFrequency() {
    return frequency;
  }
}