package org.notmysock.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.notmysock.jdbc.BenchUtils.BenchOptions;

public class JDBCActor implements Runnable {

  private final String url;
  private final int loops;
  private final int gap;

  public JDBCActor(String url, int loops, int gaptime) {
    this.url = url;
    this.loops = loops;
    this.gap = gaptime;
  }

  public static void main(String[] args) throws Exception {

    BenchOptions c = BenchUtils.getOptions(args);

    JDBCActor a = new JDBCActor(c.url, c.loops, c.gaptime);

    a.run();
  }

  @Override
  public void run() {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(this.url);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return;
    }

    PreparedStatement stmt = null;
    for (int i = 0; i < loops; i++) {
      long t0 = System.nanoTime();
      long t1 = -1;
      try {
        try {
          stmt = conn.prepareStatement("select count(1) from mostly_nulls");
          stmt.execute();
          t1 = System.nanoTime();
        } finally {
          if (stmt != null)
            stmt.close();
        }
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      long ms = TimeUnit.MILLISECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS);
      System.out.printf("[%s] Run %d - %d ms\n", this, i, ms);
      if (ms < gap) {
        try {
          Thread.sleep(gap-ms);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}
