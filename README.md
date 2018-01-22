# Hive-Kerberos-DriverWrapper
Driver wrapper to handle keytab and principal.

## Usage

1. Clone repo.
1. Update gradle.properties  
  hiveVersion=2.3.2
  hadoopVersion=1.2.1
1. Test requires docker, since kerberos is required  
   (a) With docker:  
       Build your shadowJar `gradlew clean test shadowJar`   
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