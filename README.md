# JReJSON

A Java Client Library for [ReJSON](https://github.com/redislabsmodules/rejson)

## Overview 

This client provides access to ReJSON's Redis API, and provides back-and-forth serialization between Java's and its objects.

## Installation

1. Install Jedis (once v3 is released this step will be obsolete)
    1. Clone it: `git clone --depth 1 git@github.com:xetorthio/jedis.git`
    2. `cd jedis`
    3. `mvn -Dmaven.test.skip=true install`
2. Install JReJSON (todo: add to maven)
    1. Clone it: `git clone git@github.com:RedisLabs/JReJSON.git`
    2. `cd JReJSON`
    3. `mvn -Dmaven.test.skip=true install`

## Usage example

```java
import redis.clients.jedis.Jedis;
import io.rejson.JReJSON;

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
