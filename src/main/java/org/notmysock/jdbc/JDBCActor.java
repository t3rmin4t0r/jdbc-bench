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
  private final int num;

  public JDBCActor(int num, String url, int loops, int gaptime) {
    this.url = url;
    this.loops = loops;
    this.gap = gaptime;
    this.num = num;
  }

  public static void main(String[] args) throws Exception {

    BenchOptions c = BenchUtils.getOptions(args);

    JDBCActor a = new JDBCActor(1, c.url, c.loops, c.gaptime);

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
          stmt = conn.prepareStatement("select count(1) from onerow where x=42");
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
      long wait = 0;
      if (ms < gap) {
        wait = gap - ms;
      }
      System.out.printf("[Actor #%03d] Run %d - %d ms (+%d ms)\n", num, i, ms, wait);
      if (wait > 0) {
        try {
          Thread.sleep(wait);
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
