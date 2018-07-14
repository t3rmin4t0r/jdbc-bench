package org.notmysock.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.notmysock.jdbc.BenchUtils.BenchOptions;
import org.notmysock.jdbc.BenchUtils.BenchQuery;

public class JDBCActor implements Callable<JDBCRunResult> {

  private final String url;
  private final int loops;
  private final int gap;
  public final int id;
  private final Iterator<BenchQuery> queries;
  private final JDBCRunLogger logger;

  public JDBCActor(int num, String url, int loops, int gaptime, Iterator<BenchQuery> queries, JDBCRunLogger logger) {
    this.url = url;
    this.loops = loops;
    this.gap = gaptime;
    this.id = num;
    this.queries = queries;
    this.logger = logger;
  }

  public static void main(String[] args) throws Exception {

    BenchOptions c = BenchUtils.getOptions(args);

    JDBCActor a = new JDBCActor(1, c.urls.next(), c.loops, c.gaptime, c.queries, null);

    a.call();
  }

  @Override
  public JDBCRunResult call() throws Exception {
    JDBCRunResult result = new JDBCRunResult(id, loops);
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(this.url);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return result;
    }
    if (logger != null) {
      logger.start(this);
    }

    PreparedStatement stmt = null;
    for (int i = 0; i < loops; i++) {
      long t0 = System.nanoTime();
      long t1 = -1;
      try {
        try {
          stmt = conn.prepareStatement(queries.next().contents);
          stmt.execute();
          t1 = System.nanoTime();
          result.success(t0, t1);
          if (logger != null) {
            logger.success(this, i, t0, t1);
          }
        } finally {
          if (stmt != null)
            stmt.close();
        }
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        t1 = System.nanoTime();
        result.fail(t0, t1);
        if (logger != null) {
          logger.fail(this, i, t0, t1);
        }
      }
      long ms = TimeUnit.MILLISECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS);
      long wait = 0;
      if (ms < gap) {
        wait = gap - ms;
      }
      if (wait > 0) {
        try {
          Thread.sleep(wait);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

    if (logger != null) {
      logger.end(this);
    }

    result.setConnection(conn);
    
    return result;
  }
}
