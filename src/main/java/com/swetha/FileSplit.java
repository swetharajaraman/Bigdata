package com.swetha;

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
}
