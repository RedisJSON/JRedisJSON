package io.rejson;

import org.junit.Before;
import org.junit.Test;


public class UsageExampleTest {
    @Before
    public void initialize() {
        JReJSON c = new JReJSON("localhost", 6379);
        c._conn().flushDB();
    }

    @Test
    public void exampleShouldWork() throws Exception {
        // Firstly, the client's initialization
        JReJSON c = new JReJSON("localhost", 6379);

        // Setting a Redis key name _foo_ to the string _"bar"_, and reading it back
        c.set("foo", "bar");
        String s0 = (String) c.get("foo");

        // Omitting the path (usually) defaults to the root path, so the call above to `get()` and the following ones
        // are basically interchangeable
        String s1 = (String) c.get("foo", new Path("."));
        String s2 = (String) c.get("foo", Path.RootPath());

        // Any Gson-able object can be set and updated
        c.set("obj", new Object());					         // just an empty object
        c.set("obj", null, new Path(".zilch"));
        Path p = new Path(".whatevs");
        c.set("obj", true, p);
        c.set("obj", 42, p);
        c.del("obj", p);                                    // back to almost nothing
    }
}
