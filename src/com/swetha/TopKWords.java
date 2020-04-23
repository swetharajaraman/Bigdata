package com.swetha;

import java.io.*;
import java.util.*;

public class TopKWords {
    class WordFrequency implements Comparable<WordFrequency> {
        String word;
        long frequency;

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
    }

    private int k;
    private PriorityQueue<WordFrequency> priorityQueue;
    private Map<String, Integer> wordFrequencyMap;
    private String file;

    public TopKWords(String file, int k) {
        this.k = k;
        this.priorityQueue = new PriorityQueue(k);
        wordFrequencyMap = new HashMap<>(1000000000);
        this.file = file;
        readFileAndComputeFrequencies(file);
    }

    public List<WordFrequency> topK() {
        List<WordFrequency> result = new ArrayList<>();
        while(!priorityQueue.isEmpty()) {
            result.add(priorityQueue.poll());
        }
        return result;
    }

    private void readFileAndComputeFrequencies(String file) {
        String line;
        try(BufferedReader reader = new BufferedReader(new FileReader(new File(file)))) {
            while ((line = reader.readLine()) != null) {
                String[] words = line.split("\\s+");
                for(String word : words) {
                    computeWordFrequency(word);
                }
            }
        } catch (FileNotFoundException fne) {
            System.err.println(fne.getMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        for (Map.Entry<String,Integer> entry : wordFrequencyMap.entrySet()) {
            WordFrequency wordFrequency = new WordFrequency(entry.getKey(), entry.getValue());
            addToTopKPriorityQueue(wordFrequency);
        }
    }

    private void computeWordFrequency(String word) {
        wordFrequencyMap.put(word, wordFrequencyMap.getOrDefault(word, 0) + 1);
    }

    private void addToTopKPriorityQueue(WordFrequency wordFrequency) {
        if(priorityQueue.size() <= k) {
            priorityQueue.add(wordFrequency);
        } else if(priorityQueue.peek().frequency < wordFrequency.frequency) {
            priorityQueue.poll();
            priorityQueue.add(wordFrequency);
        }
    }
}
