package org.notmysock.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.notmysock.jdbc.BenchUtils.BenchOptions;
import org.notmysock.jdbc.BenchUtils.BenchQuery;

public class JDBCActor implements Callable<JDBCRunResult> {

  public final String url;
  private final int loops;
  private final int gap;
  public final int id;
  private final Iterator<BenchQuery> queries;
  private final JDBCRunLogger logger;

  public JDBCActor(int num, String url, int loops, int gaptime,
      Iterator<BenchQuery> queries, JDBCRunLogger logger) {
    this.url = url;
    this.loops = loops;
    this.gap = gaptime;
    this.id = num;
    this.queries = queries;
    this.logger = logger;
  }

  public static void main(String[] args) throws Exception {

    BenchOptions c = BenchUtils.getOptions(args);

    JDBCActor a = new JDBCActor(1, c.urls.next(), c.loops, c.gaptime,
        c.queries, null);

    a.call();
  }

  @Override
  public JDBCRunResult call() throws Exception {
    JDBCRunResult result = new JDBCRunResult(id, loops);
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(this.url);
    } catch (SQLException e) {
      e.printStackTrace();
      return result;
    }
    if (logger != null) {
      logger.start(this);
    }

    for (int i = 0; i < loops; i++) {
      try {
        if (!runBenchmark(conn, result)) {
          break;
        }
      } catch (SQLException se) {
        // ignore
        se.printStackTrace();
        break;
      }
    }
    if (logger != null) {
      logger.end(this);
    }
    result.setConnection(conn);
    return result;
  }

  private boolean runBenchmark(Connection conn, JDBCRunResult result)
      throws SQLException {
    String queryName = "unknown";
    int i = 0;
    while (queries.hasNext()) {
      long t0 = System.nanoTime();
      long t1 = -1;
      long realTime = System.currentTimeMillis();
      PreparedStatement stmt = null;
      BenchQuery query = queries.next();
      if (query == null) {
        break;
      }
      queryName = query.name;

      try {
        stmt = conn.prepareStatement("-- " + queryName + "\n" + query.contents);
        stmt.execute();
        ResultSet rs = stmt.getResultSet();
        int r = 0;
        while (rs.next()) {
          r++;
        }
        t1 = System.nanoTime();
        result.success(t0, t1);
        if (logger != null) {
          long realMillis = TimeUnit.MILLISECONDS.convert(t1 - t0,
              TimeUnit.NANOSECONDS);
          logger.success(this, i, queryName, stmt, realTime, realTime
              + realMillis, r);
        }
      } catch (SQLException e) {
        e.printStackTrace();
        t1 = System.nanoTime();
        result.fail(t0, t1);
        long realMillis = TimeUnit.MILLISECONDS.convert(t1 - t0,
            TimeUnit.NANOSECONDS);
        if (logger != null) {
          logger
              .fail(this, i, queryName, stmt, realTime, realTime + realMillis);
        }
        return false;
      } finally {
        if (stmt != null) {
          stmt.close();
        }
      }
      i++;
      long ms = TimeUnit.MILLISECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS);
      long wait = 0;
      if (ms < gap) {
        wait = gap - ms;
      }
      if (wait > 0) {
        try {
          Thread.sleep(wait);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    return true;
  }
}
