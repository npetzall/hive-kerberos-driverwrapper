package com.github.npetzall.hive.kerberos;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.InternetProtocol;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class KDCUtil {

    private static final String PRINCIPAL = "hive/hadoop-master@NPETZALL.GITHUB.COM";

    @SuppressWarnings({"rawtypes","unchecked"})
    private final GenericContainer genericContainer;
    private final TemporaryFolder temporaryFolder;

    @SuppressWarnings({"rawtypes","unchecked"})
    public KDCUtil(GenericContainer genericContainer, TemporaryFolder temporaryFolder) {
        this.genericContainer = genericContainer;
        this.temporaryFolder = temporaryFolder;
    }

    public void setup() throws IOException {
        setupConf();
        setupPrincipalAndKeytab();
    }

    public String getConnectionUrl() {
        return "jdbc:hive2://"+genericContainer.getContainerIpAddress()+":"+getJDBCPort()+"/;principal="+PRINCIPAL;
    }

    private String getJDBCPort() {
        return getMappedPort(10000, InternetProtocol.TCP);
    }

    //Stupid OpenJDK developer not wanting to support custom port in System.properties
    //https://bugs.openjdk.java.net/browse/JDK-6795311
    private void setupConf() throws IOException {
        String realm = "NPETZALL.GITHUB.COM";
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

    private String getKDC() {
        return genericContainer.getContainerIpAddress() + ":" + getMappedPort(88, InternetProtocol.UDP);
    }

    private String getMappedPort(Integer originalPort, InternetProtocol protocol) {
        return genericContainer.getContainerInfo().getNetworkSettings().getPorts().getBindings().get(new ExposedPort(originalPort, protocol))[0].getHostPortSpec();
    }

    private void setupPrincipalAndKeytab() throws IOException {
        System.setProperty(DriverWrapper.PRINCIPAL_PROP, PRINCIPAL);
        String keyTab = temporaryFolder.newFile("keytab").getAbsolutePath();
        genericContainer.copyFileFromContainer("/etc/hive/conf/hive.keytab", keyTab);
        System.setProperty(DriverWrapper.KEYTAB_PROP,keyTab);
    }
}
