package com.github.npetzall.hive.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.jdbc.HiveDriver;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class DriverWrapper implements Driver {

  static {
    try {
      java.sql.DriverManager.registerDriver(new DriverWrapper());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }


  public static final String PRINCIPAL_PROP = "hive-kerberos-principal";
  public static final String KEYTAB_PROP = "hive-kerberos-keytab";
  public static final String HIVE_URL_PREFIX = "jdbc:hive2://";
  public static final String URL_PREFIX = "jdbc:hive2-kerberos://";

  private Driver driver;
  private ClassLoader originalClassLoader;

  public DriverWrapper() {
    try {
      setupKerberos();
      createDriver();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to wrap driver", e);
    }
  }

  private void setupKerberos() throws IOException {
    String principal = System.getProperty(PRINCIPAL_PROP);
    String keytab = System.getProperty(KEYTAB_PROP);

    if (principal == null || principal.isEmpty() || keytab == null || keytab.isEmpty()) {
      throw new IllegalArgumentException("Missing system property 'hive-kerberos-principal' or 'hive-kerberios-keytab");
    }

    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(principal, keytab);
  }

  private void createDriver() {
    driver = new HiveDriver();
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
    if (Pattern.matches(URL_PREFIX + ".*", url)) {
      url = url.replace(URL_PREFIX, HIVE_URL_PREFIX);
    }
    Connection connection = driver.connect(url, info);
    Thread.currentThread().setContextClassLoader(originalClassLoader);
    return connection;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (Pattern.matches(URL_PREFIX + ".*", url)) {
      return true;
    }
    return driver.acceptsURL(url);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return driver.getPropertyInfo(url, info);
  }

  @Override
  public int getMajorVersion() {
    return driver.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return driver.getMinorVersion();
  }

  @Override
  public boolean jdbcCompliant() {
    return driver.jdbcCompliant();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return driver.getParentLogger();
  }

}
