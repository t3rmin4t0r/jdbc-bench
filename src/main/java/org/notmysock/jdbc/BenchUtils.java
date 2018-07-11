package org.notmysock.jdbc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class BenchUtils {

  public static class BenchQuery {
    public final String name;
    public final String contents;
    
    public BenchQuery(String name, String contents) {
      this.name = name;
      this.contents = contents;
    }

    // split on the 1st "^"
    public static BenchQuery parseLine(String line) {
      int off = line.indexOf('^');
      String name = line.substring(0, off);
      String contents = line.substring(off+1);
      return new BenchQuery(name, contents);
    }
  }

  public static class BenchOptions {
    public final Iterator<String> urls;
    public final int threads;
    public final int loops;
    public final int rampup;
    public final int gaptime;
    public final Iterator<BenchQuery> queries;

    public BenchOptions(CommandLine cmd) {
      this.urls = new SynchronizedCycleIterator<String>(cmd.getOptionValues("u"));
      this.threads = Integer.parseInt(cmd.getOptionValue("t", "1"));
      this.loops = Integer.parseInt(cmd.getOptionValue("n", "10"));
      this.rampup = Integer.parseInt(cmd.getOptionValue("r", "0"));
      this.gaptime = Integer.parseInt(cmd.getOptionValue("g", "0"));
      ArrayList<BenchQuery> queries = new ArrayList<>();
      String qf = cmd.getOptionValue("qf");
      if (qf != null) {
        try {
          BufferedReader fr = new BufferedReader(new FileReader(qf), 1024*1024); // 1MB buffer for queries
          String line;
          while (null != (line = fr.readLine())) {
            queries.add(BenchQuery.parseLine(line));
          }
        } catch(IOException ioe) {
          ioe.printStackTrace();
          throw new IllegalArgumentException(String.format("Could not open query file %s", qf));
        }
      } else {
        int i = 0;
        for (String q : cmd.getOptionValues("q")) {
          queries.add(new BenchQuery(String.format("query%d", i), q));
        }
      }
      this.queries = new SynchronizedCycleIterator<BenchQuery>(
          queries.toArray(new BenchQuery[0]));
    }

    public static Options get() {
      Options options = new Options();
      options.addOption("u", true, "jdbc URL");
      options.addOption("t", true, "thread count");
      options.addOption("n", true, "iterations");
      options.addOption("r", true, "rampup in ms");
      options.addOption("g", true, "gap between loops in ms");
      options.addOption("q", true, "query");
      options.addOption("qf", true, "query file");
      return options;
    }
  }

  public static BenchOptions getOptions(String[] args) throws ParseException {

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(BenchOptions.get(), args);

    return new BenchOptions(cmd);
  }
}
