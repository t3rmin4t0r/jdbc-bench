package org.notmysock.jdbc;

import java.util.Arrays;
import java.util.stream.LongStream;

public class JDBCRunResult {

  private int id;
  private long[] samples;
  private int current = 0;

  public JDBCRunResult(int id, int samples) {
    this.id = id;
    this.samples = new long[samples];
  }

  public void success(long t0, long t1) {
    if (current < samples.length) {
      samples[current] = (t1 - t0);
      current++;
    }
  }

  public void fail(long t0, long t1) {
    if (current < samples.length) {
      samples[current] = (t0 - t1);
      current++;
    }
  }
  
  public LongStream getSamples() {
    return Arrays.stream(samples, 0, current).filter( i ->  i > 0);
  }
  
  public LongStream getErrors() {
    return Arrays.stream(samples, 0, current).filter( i ->  i < 0);
  }
  
  public int getId() {
    return id;
  }
}
