package org.notmysock.jdbc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class BenchUtils {

  public static class BenchOptions {
    public final String url;
    public final int threads;
    public final int loops;
    public final int rampup;
    public final int gaptime;

    public BenchOptions(CommandLine cmd) {
      this.url = cmd.getOptionValue("u");
      this.threads = Integer.parseInt(cmd.getOptionValue("t", "1"));
      this.loops = Integer.parseInt(cmd.getOptionValue("n", "10"));
      this.rampup = Integer.parseInt(cmd.getOptionValue("r", "0"));
      this.gaptime = Integer.parseInt(cmd.getOptionValue("g", "0"));
    }

    public static Options get() {
      Options options = new Options();
      options.addOption("u", true, "jdbc URL");
      options.addOption("t", true, "thread count");
      options.addOption("n", true, "iterations");
      options.addOption("r", true, "rampup in ms");
      options.addOption("g", true, "gap between loops in ms");
      return options;
    }
  }

  public static BenchOptions getOptions(String[] args) throws ParseException {

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(BenchOptions.get(), args);

    return new BenchOptions(cmd);
  }
}
