package com.github.npetzall.hive.kerberos;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.InternetProtocol;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.HostPortWaitStrategy;
import org.testcontainers.containers.wait.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.WaitAllStrategy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class DriverWrapperHadoopTest {

    private static final String PRINCIPAL = "hive/hadoop-master@LABS.TERADATA.COM";

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule
    @SuppressWarnings({"rawtypes","unchecked"})
    public static GenericContainer CDH5_HIVE_KERBEROS =
            new GenericContainer("teradatalabs/cdh5-hive-kerberized:latest")
            .withCreateContainerCmdModifier((createContainerCmd) -> {
                ((CreateContainerCmd)createContainerCmd)
                        .withExposedPorts(new ExposedPort(88, InternetProtocol.UDP))
                .withHostName("hadoop-master");
            }).waitingFor(new WaitAllStrategy()
                    .withStrategy(new LogMessageWaitStrategy().withRegEx(".*bootstrap \\(exit status 0; expected\\)\n"))
                    .withStrategy(new HostPortWaitStrategy()))
            .withExposedPorts(88,10000);

    @BeforeClass
    public static void setupKerberos() throws IOException {
        setupConf();
        setupPrincipalAndKeytab();
    }

    @Test
    public void canCreateDriverWrapper() {
        DriverWrapper driverWrapper = new DriverWrapper();
        assertThat(driverWrapper).isNotNull();
    }

    @Test
    public void canConnectAndCreateTable() throws SQLException {
        DriverWrapper driverWrapper = new DriverWrapper();
        try (Connection connection = driverWrapper.connect(getConnectionUrl(), new Properties())){
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table hiveKerberosTest (key int, value string)");
                try (ResultSet resultSet = connection.getMetaData().getTables(null, "default", null, new String[]{"TABLES"})) {
                    resultSet.next();
                    String tableName = resultSet.getString("TABLE_NAME");
                    assertThat(tableName).isEqualToIgnoringCase("hiveKerberosTest");
                }
            }
        }
    }

    private String getConnectionUrl() {
        return "jdbc:hive2://"+CDH5_HIVE_KERBEROS.getContainerIpAddress()+":"+getJDBCPort()+"/;principal="+PRINCIPAL;
    }

    private String getJDBCPort() {
        return getMappedPort(10000, InternetProtocol.TCP);
    }

    //Stupid OpenJDK developer not wanting to support custom port in System.properties
    //https://bugs.openjdk.java.net/browse/JDK-6795311
    private static void setupConf() throws IOException {
        String realm = "LABS.TERADATA.COM";
        String kdc = getKDC();
        File krb5Conf = temporaryFolder.newFile();
        try (PrintWriter printWriter = new PrintWriter(new FileWriter(krb5Conf))) {
            printWriter.println("[libdefaults]\n" +
                    "dns_lookup_realm = false\n" +
                    "dns_lookup_kdc = false\n" +
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

    private static String getKDC() {
        return CDH5_HIVE_KERBEROS.getContainerIpAddress() + ":" + getMappedPort(88, InternetProtocol.UDP);
    }

    private static String getMappedPort(Integer originalPort, InternetProtocol protocol) {
        return CDH5_HIVE_KERBEROS.getContainerInfo().getNetworkSettings().getPorts().getBindings().get(new ExposedPort(originalPort, protocol))[0].getHostPortSpec();
    }

    private static void setupPrincipalAndKeytab() throws IOException {
        System.setProperty(DriverWrapper.PRINCIPAL_PROP, PRINCIPAL);
        String keyTab = temporaryFolder.newFile("keytab").getAbsolutePath();
        CDH5_HIVE_KERBEROS.copyFileFromContainer("/etc/hive/conf/hive.keytab", keyTab);
        System.setProperty(DriverWrapper.KEYTAB_PROP,keyTab);
    }
}
