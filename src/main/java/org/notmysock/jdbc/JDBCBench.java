package org.notmysock.jdbc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.ParseException;
import org.notmysock.jdbc.BenchUtils.BenchOptions;

public class JDBCBench {
  public static void main(String[] args) throws ParseException, InterruptedException, ExecutionException, IOException {
    BenchOptions opts = BenchUtils.getOptions(args);
    ExecutorService threads = Executors.newFixedThreadPool(opts.threads);
    if (opts.warmups > 0) {
      for (int i = 0; i < opts.warmups; i++) {
        JDBCActor actor = new JDBCActor(i, opts.urls.next(), opts.loops, 0,
            opts.queries, null);
        try {
          actor.call();
        } catch (Exception e) {
          e.printStackTrace();
          // continue in all cases
        }
      }
    }
    JDBCRunLogger logger = new JDBCRunLogger(opts);
    ArrayList<Future<JDBCRunResult>> results = new ArrayList<Future<JDBCRunResult>>(opts.threads);
    for (int i = 0; i < opts.threads; i++) {
      results.add(threads.submit(new JDBCActor(i, opts.urls.next(), opts.loops, opts.gaptime, opts.queries, logger)));
      if (opts.rampup > 0) {
        Thread.sleep(opts.rampup);
      }
    }
    threads.shutdown();
    
    long max = -1, min = Long.MAX_VALUE, sum = 0, count = 0, errors = 0;
    
    for(Future<JDBCRunResult> f : results) {
      JDBCRunResult r = f.get();
      System.out.println(r);

      max = Math.max(max, r.getSamples().max().getAsLong());
      min = Math.min(min, r.getSamples().min().getAsLong());
      sum = sum + r.getSamples().sum();
      count = count + r.getSamples().count();
      errors = errors + r.getErrors().count();

      r.close();
    }
    System.out.flush();
    
    System.out.printf("With %d users (x %d loops) : errors = %d, avg=%d ms, best=%d ms, worst=%d ms\n", opts.threads, opts.loops, errors, (sum/count),  min, max);
  }
}
