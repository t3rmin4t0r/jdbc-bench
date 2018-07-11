package org.notmysock.jdbc;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Date;

import org.notmysock.jdbc.BenchUtils.BenchOptions;

public class JDBCRunLogger {

  final FileWriter out;
  
  public JDBCRunLogger(BenchOptions opts) throws IOException {
    String log = String.format("run_%s_%d_users_x_%d_loops.csv", LocalTime.now().toString(), opts.threads, opts.loops);
    this.out = new FileWriter(log);
    write("User, Loop, Start, End, Success");
  }

  private void write(String string) {
    try {
      out.write(string+"\n");
      out.flush();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void fail(JDBCActor jdbcActor, int i, long t0, long t1) {
    write(String.format("user-%d, %d, %d, %d, %d, %d, false", jdbcActor.id, i, t0, t1, (t1-t0)));
  }

  public void success(JDBCActor jdbcActor, int i, long t0, long t1) {
    write(String.format("user-%d, %d, %d, %d, %d, %d, true", jdbcActor.id, i, t0, t1, (t1-t0)));
  }

}
