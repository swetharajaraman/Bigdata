package com.swetha;

import java.io.Serializable;

public class WordFrequency implements Comparable<WordFrequency>, Serializable {
  String word;
  long frequency;

  public WordFrequency() {
    word = null;
    frequency = 0L;
  }

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