package com.swetha;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        String file = args[0];
        List<TopKWords.WordFrequency> topKWords = new TopKWords(file, 10).topK();
        System.out.println(topKWords);
    }
}
