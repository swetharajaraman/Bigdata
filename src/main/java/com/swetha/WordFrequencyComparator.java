package com.swetha;

import java.util.Comparator;

public class WordFrequencyComparator implements Comparator<WordFrequency> {
  @Override
  public int compare(WordFrequency o1, WordFrequency o2) {
    return (int) (o1.getFrequency() - o2.getFrequency());
  }
}