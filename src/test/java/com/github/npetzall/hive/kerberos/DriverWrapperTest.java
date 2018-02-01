package com.github.npetzall.hive.kerberos;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.InternetProtocol;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.HostPortWaitStrategy;
import org.testcontainers.containers.wait.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.WaitAllStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import javax.sound.midi.Patch;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class DriverWrapperTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule
    @SuppressWarnings({"rawtypes","unchecked"})
    public static GenericContainer CDH5_HIVE_KERBEROS =
            new GenericContainer(
                    "hdp-hive-kerberized:latest"
            ).withCreateContainerCmdModifier((createContainerCmd) -> {
        ((CreateContainerCmd)createContainerCmd)
                .withHostName("hadoop-master").withPortBindings(new PortBinding(new Ports.Binding("0.0.0.0","10000"),new ExposedPort(10000, InternetProtocol.TCP)));
    }).waitingFor(new WaitAllStrategy()
            .withStrategy(new LogMessageWaitStrategy().withRegEx(".*bootstrap \\(exit status 0; expected\\)\n"))
            .withStrategy(new HostPortWaitStrategy()))
            .withExposedPorts(88,2181,10000);

    private static final KDCUtil kdcUtil = new KDCUtil(CDH5_HIVE_KERBEROS, temporaryFolder);

    @BeforeClass
    public static void setupKDC() throws IOException {
        kdcUtil.setup();
    }

    @Test
    public void canCreateDriverWrapper() {
        DriverWrapper driverWrapper = new DriverWrapper();
        assertThat(driverWrapper).isNotNull();
    }

    @Test
    public void canConnectAndCreateTable() throws SQLException {
        DriverWrapper driverWrapper = new DriverWrapper();
        try (Connection connection = driverWrapper.connect(kdcUtil.getConnectionUrl(), new Properties())){
            JDBCUtil.createAndValidateTable(connection);
        }
    }

    @Test
    public void canConnectWithZookeeperAndCreateTable() throws SQLException {
        DriverWrapper driverWrapper = new DriverWrapper();
        try (Connection connection = driverWrapper.connect(getConnectionUrl(), new Properties())){
            JDBCUtil.createAndValidateTable(connection);
        }
    }

    private String getConnectionUrl() {
        return "jdbc:hive2://"+CDH5_HIVE_KERBEROS.getContainerIpAddress()+":"+CDH5_HIVE_KERBEROS.getMappedPort(2181)+"/;serviceDiscoveryMode=zooKeeper";
    }
}
