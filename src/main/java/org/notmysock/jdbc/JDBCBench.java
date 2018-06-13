package org.notmysock.jdbc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.ParseException;
import org.notmysock.jdbc.BenchUtils.BenchOptions;

public class JDBCBench {
  public static void main(String[] args) throws ParseException {
    BenchOptions opts = BenchUtils.getOptions(args);
    ExecutorService threads = Executors.newFixedThreadPool(opts.threads);
    for (int i = 0; i < opts.threads; i++) {
      threads.submit(new JDBCActor(opts.url, opts.loops));
    }
    threads.shutdown();
  }
}
