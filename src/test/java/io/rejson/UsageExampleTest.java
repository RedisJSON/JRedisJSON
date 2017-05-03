package io.rejson;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;


public class UsageExampleTest {

    private String host="52.7.3.128";
    private int port=6379;
    Jedis jedis  = new Jedis(host,port);

    @Before
    public void initialize() {
        jedis.flushDB();
    }

    @Test
    public void exampleShouldWork() throws Exception {


        // Setting a Redis key name _foo_ to the string _"bar"_, and reading it back
        JReJSON.set(jedis,"foo", "bar");
        String s0 = (String) JReJSON.get(jedis,"foo");

        // Omitting the path (usually) defaults to the root path, so the call above to `get()` and the following ones
        // are basically interchangeable
        String s1 = (String) JReJSON.get(jedis,"foo", new Path("."));
        String s2 = (String) JReJSON.get(jedis, "foo", Path.RootPath());

        // Any Gson-able object can be set and updated
        JReJSON.set(jedis,"obj", new Object());					         // just an empty object
        JReJSON.set(jedis,"obj", null, new Path(".zilch"));
        Path p = new Path(".whatevs");
        JReJSON.set(jedis,"obj", true, p);
        JReJSON.set(jedis,"obj", 42, p);
        JReJSON.del(jedis,"obj", p);                                    // back to almost nothing
    }
}
