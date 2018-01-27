# Hive-Kerberos-DriverWrapper
Driver wrapper to handle keytab and principal.

## Usage

1. Clone repo.
1. Update gradle.properties  
  distributionRepo=https://repository.cloudera.com/content/repositories/releases/  
  hiveVersion=1.1.0-cdh5.9.1  
  hadoopVersion=2.6.0-cdh5.9.1  
1. Test requires docker, since kerberos is required  
   (a) With docker: 
       Build the docker dockerfile located in the docker folder. `./build.sh`  
       Sadly you need to add hadoop-master to you hosts file pointing towards you docker (127.0.0.1)
       Build your shadowJar `gradlew clean check shadowJar`   
   (b) Without docker:  
       Build your shadowJar `gradlew clean shadowJar`
1. Use the shadow-jar, specify principal and keytabs using System properties
`-Dhive-kerberos-principal=` `-Dhive-kerberos-keytab=`

There might be issues when using DriverManager(since this and HiveDriver both accepts same url)

So use 
`(Driver)Class.for("com.github.npetzall.hive.kerberos.DriverWrapper").newInstance()`

Or replace the jdbc prefix used by Hive with the wrapper one
replace `jdbc:hive2://` with `jdbc:hive2-kerberos://` it will be switched back before HiveDriver is called.

All methods are delegated to HiveDriver.


### Class not found
This might happen you need to switch transitive on one of the compile dependencies.
For cloudera this is needed for hadoop-auth to get curator.
Setting it hadoop-common to transitive = true failes the build due to missing dependency.

### Missing dependency/failed to download
See Class not found might need to experiment with the transitiv settings.