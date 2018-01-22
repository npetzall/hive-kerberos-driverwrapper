package com.github.npetzall.hive.kerberos;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class DriverWrapper implements Driver {

  private Driver driver;

  public DriverWrapper() {
    try {
      setupKerberos();
      createDriver();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to wrap driver", e);
    }
  }

  private void setupKerberos() throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
    Class<?> hadoopConfConfigurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
    Object hadoopConfConfiguration = hadoopConfConfigurationClass.newInstance();
    Method hadoopConfConfigurationSet = hadoopConfConfigurationClass.getDeclaredMethod("set", String.class, String.class);
    hadoopConfConfigurationSet.invoke(hadoopConfConfiguration, "hadoop.security.authentication", "kerberos");

    Class<?> userGroupInformationClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");

    Method userGroupInforamtionSetConfiguration = userGroupInformationClass.getDeclaredMethod("setConfiguration", hadoopConfConfigurationClass);
    userGroupInforamtionSetConfiguration.invoke(userGroupInformationClass, hadoopConfConfiguration);

    String principal = System.getProperty("hive-kerberos-principal");
    String keytab = System.getProperty("hive-kerberos-keytab");

    if (principal == null || principal.isEmpty() || keytab == null || keytab.isEmpty()) {
      throw new IllegalArgumentException("Missing system property 'hive-kerberos-principal' or 'hive-kerberios-keytab");
    }

    Method userGroupInformationLoginUserFromKeytab = userGroupInformationClass.getDeclaredMethod("loginUserFromKeytab", String.class, String.class);
    userGroupInformationLoginUserFromKeytab.invoke(userGroupInformationClass, principal, keytab);
  }

  private void createDriver() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    String driverClass = System.getProperty("hive-driver", "org.apache.hive.jdbc.HiveDriver");
    Class<Driver> driverClazz = (Class<Driver>) Class.forName(driverClass);
    driver = driverClazz.newInstance();
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return driver.connect(url, info);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
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
