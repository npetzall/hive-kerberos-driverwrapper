FROM teradatalabs/cdh5-hive-kerberized:latest

RUN /usr/sbin/kadmin.local -q "addprinc -randkey zookeeper/hadoop-master@LABS.TERADATA.COM"
RUN /usr/sbin/kadmin.local -q "addprinc -randkey zkcli@LABS.TERADATA.COM"

RUN /usr/sbin/kadmin.local -q "xst -norandkey -k /etc/zookeeper/conf/zookeeper.keytab zookeeper/hadoop-master"
RUN chown zookeeper:zookeeper /etc/zookeeper/conf/zookeeper.keytab
RUN /usr/sbin/kadmin.local -q "xst -norandkey -k /etc/zookeeper/conf/zkcli.keytab zkcli@LABS.TERADATA.COM"
RUN echo "authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider" >> /etc/zookeeper/conf/zoo.cfg
RUN echo "jaasLoginRenew=3600000" >> /etc/zookeeper/conf/zoo.cfg
RUN echo "export JVMFLAGS=\"-Djava.security.auth.login.config=/etc/zookeeper/conf/jaas.conf\"" >> /etc/zookeeper/conf/java.env

ADD zookeeper/jaas.conf /etc/zookeeper/conf/
ADD hive/hive-site.xml /etc/hive/conf/

EXPOSE 88/udp 2181

CMD /root/startup.sh