package com.swetha;

import java.util.ArrayList;
import java.util.List;


public class FileSplit {
  int id;
  long start;
  long length;
  String file;

  public FileSplit(int id, long start, long length, String file) {
    this.id = id;
    this.start = start;
    this.length = length;
    this.file = file;
  }

  @Override
  public String toString() {
    return "File Split with file " + file + " starting offset " + start + " end " + (start + length);
  }

  public static List<FileSplit> getSplits(String file, long fileLength, long splitSize) {
    int i = 0;
    final double SPLIT_SLOP = 1.1;
    List<FileSplit> splits = new ArrayList<>();
    long bytesRemaining = fileLength;
    while(((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
      FileSplit split = new FileSplit(i, fileLength - bytesRemaining, splitSize, file);
      splits.add(split);
      bytesRemaining -= splitSize;
      i++;
    }

    if(bytesRemaining != 0) {
      FileSplit split = new FileSplit(i, fileLength - bytesRemaining, splitSize, file);
      splits.add(split);
    }
    return splits;
  }
}
