# JReJSON

A Java Client Library for [ReJSON](https://github.com/redislabsmodules/rejson)

## Overview 

This client provides access to ReJSON's Redis API, and provides back-and-forth serialization between Java's and its objects.

## Usage example

Firstly, the client's initialization:

```java
Client c = new Client("localhost", 6379);
```

Setting a Redis key name _foo_ to the string _"bar"_, and reading it back:
```java
c.set("foo", "bar");
String s0 = (String) c.get("foo");
```

Omitting the path (usually) defaults to the root path, so the call above to `get()` and the following ones are basically interchangeable:

```java
String s1 = (String) c.get("foo", new Path("."));
String s2 = (String) c.get("foo", Path.RootPath());
```

Any Gson-able object can be set and updated:
```java
c.set("obj", new Object());                         // just an empty object
c.set("obj", null, new Path(".zilch"));
Path p = new Path(".whatevs");
c.set("obj", true, p);
c.set("obj", 42, p);
c.del("obj", p);                                    // back to almost nothing
```
