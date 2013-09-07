## MySQLFactory

Mock is good for many testing situration. To simulate a real MySQL instance is difficlut. The project provides a Java API to create MySQL database instances on the fly.

## Usage


```java
/* we need the mysql tools in its installation directory */
MySQLManager manager = new MySQLManager(MYSQL_SOFTWARE_PATH);
```

Create the instance from MySQLManager and get the connection url

```java
MySQLInstance instance = manager.createDatabase();
String baseUrl = instance.getBaseConnectionUrl();
```

Tear down MySQL instances

```java

/* shutdown an instance */
instance.shutdown();

/* close all managed instances */
manager.close();
```

## Limitation

Windows is not supported.
