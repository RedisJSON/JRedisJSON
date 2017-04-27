package io.rejson;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.*;

class FooBarObject extends Object {
    public String foo;

    public FooBarObject() {
        this.foo = "bar";
    }
}

public class ClientTest {

    private Client c;

    @Before
    public void initialize() {
        c = new Client("localhost", 6379);
    }

    @Test
    public void set() throws Exception {
        c._conn().flushDB();

        c.set("null", Path.RootPath(), null);
        c.set("foobar", Path.RootPath(), new FooBarObject());
    }

    @Test(expected = Exception.class)
    public void setException() throws Exception {
        c._conn().flushDB();

        // should error on non root path for new key
        c.set("test", new Path(".foo"), "bar");
    }

    @Test
    public void get() throws Exception {
        c._conn().flushDB();
        c.set("test", Path.RootPath(), "foo");
        assertEquals("foo", c.get("test", Path.RootPath()));
    }

    @Test(expected = Exception.class)
    public void getException() throws Exception {
        c._conn().flushDB();
        c.set("test", Path.RootPath(), "foo");
        c.get("test", new Path(".bar"));
    }

    @Test
    public void del() throws Exception {
        c._conn().flushDB();
        c.set("foobar", Path.RootPath(), new FooBarObject());
        c.del("foobar", new Path(".foo"));
    }

    @Test(expected = Exception.class)
    public void delException() throws Exception {
        c._conn().flushDB();
        c.set("foobar", Path.RootPath(), new FooBarObject());
        c.del("foobar", new Path(".foo[1]"));
    }

    @Test
    public void type() throws Exception {
        c._conn().flushDB();
        c.set("foobar", Path.RootPath(), new FooBarObject());
        assertSame(Object.class, c.type("foobar", Path.RootPath()));
        assertSame(String.class, c.type("foobar", new Path(".foo")));
    }

    @Test(expected = Exception.class)
    public void typeException() throws Exception {
        c._conn().flushDB();
        c.set("foobar", Path.RootPath(), new FooBarObject());
        c.type("foobar", new Path(".foo[1]"));
    }

}