package com.swetha;

import java.util.ArrayList;
import java.util.List;


public class FileSplit {
  long start;
  long length;
  String file;

  public FileSplit(long start, long length, String file) {
    this.start = start;
    this.length = length;
    this.file = file;
  }

  @Override
  public String toString() {
    return "File Split with file " + file + " starting offset " + start + " end " + (start + length);
  }

  public static List<FileSplit> getSplits(String file, long fileLength, long splitSize) {
    long start = 0;
    List<FileSplit> splits = new ArrayList<>();
    while(fileLength > 0) {
      FileSplit split = new FileSplit(start, splitSize, file);
      splits.add(split);
      start += splitSize + 1;
      fileLength -= splitSize;
    }
    return splits;
  }
}
