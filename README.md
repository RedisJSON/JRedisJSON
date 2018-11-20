[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jrejson/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jrejson)
[![License](https://img.shields.io/badge/License-BSD%202--Clause-orange.svg)](https://opensource.org/licenses/BSD-2-Clause)

# JReJSON

A Java Client Library for [ReJSON](https://github.com/redislabsmodules/rejson)

## Overview 

This client provides access to ReJSON's Redis API, and provides back-and-forth serialization between Java's and its objects.

This project is currently WIP and the interface may change. Also note that only the core ReJSON commands are supported at the moment. 



## Official Releases

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jrejson</artifactId>
      <version>1.0.0</version>
    </dependency>
  </dependencies>
```

### Snapshots

```xml
  <repositories>
    <repository>
      <id>snapshots-repo</id>
      <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
  </repositories>
```

and

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jrejson</artifactId>
      <version>1.0.1-SNAPSHOT</version>
    </dependency>
  </dependencies>
```


## Build
    1. Clone it: `git clone git@github.com:RedisLabs/JReJSON.git`
    2. `cd JReJSON`
    3. `mvn clean install -Dmaven.test.skip=true`

## Usage example

```java  
import redis.clients.jedis.Jedis;
import com.redislabs.modules.rejson.JReJSON;

// First get a connection
Jedis jedis = new Jedis("localhost", 6379);

// Setting a Redis key name _foo_ to the string _"bar"_, and reading it back
JReJSON.set(jedis,"foo", "bar");
String s0 = (String) JReJSON.get(jedis,"foo");

// Omitting the path (usually) defaults to the root path, so the call above to
// `get()` and the following ones // are basically interchangeable
String s1 = (String) JReJSON.get(jedis,"foo", new Path("."));
String s2 = (String) JReJSON.get(jedis, "foo", Path.RootPath());

// Any Gson-able object can be set and updated
JReJSON.set(jedis,"obj", new Object());             // just an empty object
JReJSON.set(jedis,"obj", null, new Path(".zilch"));
Path p = new Path(".whatevs");
JReJSON.set(jedis,"obj", true, p);
JReJSON.set(jedis,"obj", 42, p);
JReJSON.del(jedis,"obj", p);                        // back to almost nothing

```

# Contributing

Please use this repository's issue tracker to submit issues and pull requests.

# License

[BSD 2-Clause License](LICENSE)

