package com.github.npetzall.hive.kerberos;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.InternetProtocol;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class DriverWrapperTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  @SuppressWarnings({"rawtypes","unchecked"})
  public static GenericContainer kerberosServer = new GenericContainer(
          new ImageFromDockerfile()
                  .withFileFromPath("/", Paths.get("docker/kerberos-server")));

  @Test
  public void canCreateWrapper() throws IOException {
    setupConf();
    System.setProperty(DriverWrapper.PRINCIPAL_PROP, "user/user@NPETZALL.COM");
    System.setProperty(DriverWrapper.KEYTAB_PROP,"src/test/resources/user.rc4hmac.keytab");
    DriverWrapper driverWrapper = new DriverWrapper();
    assertThat(driverWrapper).isNotNull();
  }

  //Stupid OpenJDK developer not wanting to support custom port in System.properties
  //https://bugs.openjdk.java.net/browse/JDK-6795311
  private void setupConf() throws IOException {
    String realm = "NPETZALL.COM";
    String kdc = getKDC();
    File krb5Conf = temporaryFolder.newFile();
    try (PrintWriter printWriter = new PrintWriter(new FileWriter(krb5Conf))) {
      printWriter.println("[libdefaults]\n" +
              "dns_lookup_realm = false\n" +
              "ticket_lifetime = 24h\n" +
              "renew_lifetime = 7d\n" +
              "forwardable = true\n" +
              "rdns = false\n" +
              "default_realm = " + realm + "\n" +
              "\n" +
              "[realms]\n" +
              realm + " = {");
      printWriter.println("kdc = " + kdc);
      printWriter.println("}");
      printWriter.flush();
    }
    System.setProperty("java.security.krb5.conf",krb5Conf.getAbsolutePath());
  }

  private String getKDC() {
    return kerberosServer.getContainerIpAddress() + ":" + getMappedPort(88, InternetProtocol.UDP);
  }

  private String getMappedPort(Integer originalPort, InternetProtocol protocol) {
    return kerberosServer.getContainerInfo().getNetworkSettings().getPorts().getBindings().get(new ExposedPort(originalPort, protocol))[0].getHostPortSpec();
  }

}