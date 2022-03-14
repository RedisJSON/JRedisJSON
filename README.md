[![license](https://img.shields.io/github/license/RedisJSON/jredisjson.svg)](https://github.com/RedisJSON/JRedisJSON/blob/master/LICENSE)
[![CircleCI](https://circleci.com/gh/RedisJSON/JRedisJSON/tree/master.svg?style=svg)](https://circleci.com/gh/RedisJSON/JRedisJSON/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RedisJSON/JRedisJSON.svg)](https://github.com/RedisJSON/JRedisJSON/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jrejson/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jrejson)
[![Javadocs](https://www.javadoc.io/badge/com.redislabs/jrejson.svg)](https://www.javadoc.io/doc/com.redislabs/jrejson)
[![Codecov](https://codecov.io/gh/RedisJSON/JRedisJSON/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisJSON/JRedisJSON)

[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/RedisJSON/JRedisJSON.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisJSON/JRedisJSON/context:java)
[![Known Vulnerabilities](https://snyk.io/test/github/RedisJSON/JRedisJSON/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/RedisJSON/JRedisJSON?targetFile=pom.xml)


# JRedisJSON

[![Forum](https://img.shields.io/badge/Forum-RedisJSON-blue)](https://forum.redislabs.com/c/modules/redisjson)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/QUkjSsk)
    

A Java Client Library for [RedisJSON](https://github.com/RedisJSON/RedisJSON)

## Deprecation notice

As of [Jedis 4.0.0](https://github.com/redis/jedis) this library is deprecated. It's features have been merged into Jedis. Please either install it from [maven](https://mvnrepository.com/artifact/redis.clients/jedis) or [the repo](https://github.com/redis/jedis).

## Overview 

This client provides access to RedisJSON's Redis API, and provides back-and-forth serialization between Java's and its objects.

This project is currently WIP and the interface may change. Also note that only the core RedisJSON commands are supported at the moment. 



## Official Releases

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jrejson</artifactId>
      <version>1.4.0</version>
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
      <version>1.5.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```


## Build
    1. Clone it: `git clone git@github.com:RedisJSON/JRedisJSON.git`
    2. `cd JRedisJSON`
    3. `mvn clean install -Dmaven.test.skip=true`

## Usage example

```java  
import redis.clients.jedis.Jedis;
import com.redislabs.modules.rejson.JReJSON;

// First get a connection
JReJSON client = new JReJSON("localhost", 6379);

// Setting a Redis key name _foo_ to the string _"bar"_, and reading it back
client.set("foo", "bar");
String s0 = (String) client.get("foo");

// Omitting the path (usually) defaults to the root path, so the call above to
// `get()` and the following ones // are basically interchangeable
String s1 = (String) client.get("foo", new Path("."));
String s2 = (String) client.get("foo", Path.ROOT_PATH);

// Any Gson-able object can be set and updated
client.set("obj", new Object());             // just an empty object
client.set("obj", null, new Path(".zilch"));
Path p = new Path(".whatevs");
client.set("obj", true, p);
client.set("obj", 42, p);
client.del("obj", p);                        // back to almost nothing

```

# Contributing

Please use this repository's issue tracker to submit issues and pull requests.

# License

[BSD 2-Clause License](LICENSE)

