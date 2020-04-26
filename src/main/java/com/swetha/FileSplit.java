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
    final double SPLIT_SLOP = 1.1;
    List<FileSplit> splits = new ArrayList<>();
    long bytesRemaining = fileLength;
    while(((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
      FileSplit split = new FileSplit(fileLength - bytesRemaining, splitSize, file);
      splits.add(split);
      bytesRemaining -= splitSize;
    }

    if(bytesRemaining != 0) {
      FileSplit split = new FileSplit(fileLength - bytesRemaining, splitSize, file);
      splits.add(split);
    }
    return splits;
  }
}
