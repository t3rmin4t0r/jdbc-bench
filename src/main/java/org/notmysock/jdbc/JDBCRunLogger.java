package org.notmysock.jdbc;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hive.jdbc.HiveStatement;
import org.notmysock.jdbc.BenchUtils.BenchOptions;

public class JDBCRunLogger {

  final FileWriter out;
  final AtomicLong count = new AtomicLong(0);
  final AtomicLong totalTime  = new AtomicLong(0);
  final AtomicLong activeSessions = new AtomicLong(0);

  final long zero = System.nanoTime();
  
  public JDBCRunLogger(BenchOptions opts) throws IOException {
    String log = String.format("run_%s_%s__%d_users_x_%d_loops.csv", LocalDate.now().toString(), LocalTime.now().toString(), opts.threads, opts.loops);
    this.out = new FileWriter(log);
    write("User, Loop, Start, End, Success");
  }

  private synchronized void  write(String string) {
    try {
      out.write(string);
      out.write("\n");
      out.flush();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void start(JDBCActor jdbcActor) {
    activeSessions.incrementAndGet();
  }
  
  private String getQueryId(Statement stmt) {
    String queryId = "unknown";
    if (stmt instanceof HiveStatement) {
      try {
        queryId = ((HiveStatement)stmt).getQueryId();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return queryId;
  }

  public void fail(JDBCActor jdbcActor, int i, Statement stmt, long t0, long t1) {
    count.incrementAndGet();
    write(String.format("user-%d, %d, %s, %d, %d, %d, false, -1", jdbcActor.id, i, getQueryId(stmt), t0, t1, (t1-t0)));
  }

  public void success(JDBCActor jdbcActor, int i, Statement stmt, long t0, long t1, int rows) {
    totalTime.addAndGet(java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(t1-t0));
    if(count.incrementAndGet() % 10 == 1) {
      long t = (System.nanoTime() - zero)/(1000_000_000L);
      System.out.printf("[%9d s] ActiveSessions: %9d, Queries Finished: %9d, Average time: %9d\r", t, activeSessions.get(), count.get(), totalTime.get()/count.get());
    }
    write(String.format("user-%d, %d, %s, %d, %d, %d, true, %d", jdbcActor.id, i, getQueryId(stmt), t0, t1, (t1-t0), rows));
  }
  
  public void end(JDBCActor jdbcActor) {
    activeSessions.decrementAndGet();
  }

}
