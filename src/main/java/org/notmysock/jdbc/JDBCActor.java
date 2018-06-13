package org.notmysock.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

public class JDBCActor implements Runnable {

  public final String url;

  public JDBCActor(String url) {
    this.url = url;
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("u", true, "jdbc URL");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);
    
    String url = cmd.getOptionValue("u");
    
    JDBCActor a = new JDBCActor(url);
    
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
    for (int i = 0; i < 10; i++) {
      long t0 = System.nanoTime();
      long t1 = -1;
      try {
        try {
          stmt = conn.prepareStatement("select count(1) from mostly_nulls");
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
      System.out.printf("Run %d - %d ns\n", i, (t1-t0));
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
