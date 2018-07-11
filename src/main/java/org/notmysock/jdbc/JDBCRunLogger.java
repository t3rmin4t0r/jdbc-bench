package org.notmysock.jdbc;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.atomic.AtomicLong;

import org.notmysock.jdbc.BenchUtils.BenchOptions;

public class JDBCRunLogger {

  final FileWriter out;
  final AtomicLong count = new AtomicLong(0);
  final AtomicLong activeSessions = new AtomicLong(0);
  
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

  public void fail(JDBCActor jdbcActor, int i, long t0, long t1) {
    count.incrementAndGet();
    write(String.format("user-%d, %d, %d, %d, %d, false", jdbcActor.id, i, t0, t1, (t1-t0)));
  }

  public void success(JDBCActor jdbcActor, int i, long t0, long t1) {
    if(count.incrementAndGet() % 10 == 0) {
      System.out.printf("ActiveSessions: %9d, Queries Finished: %9d\r", activeSessions.get(), count.get());
    }
    write(String.format("user-%d, %d, %d, %d, %d, true", jdbcActor.id, i, t0, t1, (t1-t0)));
  }
  
  public void end(JDBCActor jdbcActor) {
    activeSessions.decrementAndGet();
  }

}
