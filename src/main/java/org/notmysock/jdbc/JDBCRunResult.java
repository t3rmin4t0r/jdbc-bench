package org.notmysock.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

public class JDBCRunResult implements AutoCloseable {

  private int id;
  private long[] samples;
  private int current = 0;
  private Connection connection;

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
    return Arrays.stream(samples, 0, current).filter( i ->  i > 0).map(t -> TimeUnit.NANOSECONDS.toMillis(t));
  }
  
  public LongStream getErrors() {
    return Arrays.stream(samples, 0, current).filter( i ->  i < 0).map(t -> TimeUnit.NANOSECONDS.toMillis(t));
  }
  
  public int getId() {
    return id;
  }

  public void setConnection(Connection conn) {
    this.connection = conn;
  }
  
  public Connection getConnection() {
    return this.connection;
  }


  public String toString() {
  	return String.format("[Actor #%d] end (loops=%d, avg = %d ms)", getId(), getSamples().count(), getSamples().sum()/getSamples().count());
  }
  
  @Override
  public void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } finally {
        this.connection = null;
      }
    }
  }
}
