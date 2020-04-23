package com.swetha;

import java.util.Comparator;

public class WordFrequencyComparator implements Comparator<TopKWords.WordFrequency> {
    @Override
    public int compare(TopKWords.WordFrequency o1, TopKWords.WordFrequency o2) {
        return (int) (o1.frequency - o2.frequency);
    }
}