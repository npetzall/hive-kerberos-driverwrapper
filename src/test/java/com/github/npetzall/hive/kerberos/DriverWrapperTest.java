package com.github.npetzall.hive.kerberos;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DriverWrapperTest {

  @Test
  public void canCreateWrapper() {
    System.setProperty("hive-kerberos-principal", "the principal");
    System.setProperty("hive-kerberos-keytab","the path to keytab");
    DriverWrapper driverWrapper = new DriverWrapper();
    assertThat(driverWrapper).isNotNull();
  }

}