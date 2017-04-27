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

        c.set("null", ".", null);
        c.set("foobar", ".", new FooBarObject());
    }

    @Test(expected = Exception.class)
    public void setException() throws Exception {
        c._conn().flushDB();

        // should error on non root path for new key
        c.set("test", ".foo", "bar");
    }

    @Test
    public void get() throws Exception {
        c._conn().flushDB();
        c.set("test", ".", "foo");
        assertEquals("foo", c.get("test", "."));
    }

    @Test(expected = Exception.class)
    public void getException() throws Exception {
        c._conn().flushDB();
        c.set("test", ".", "foo");
        c.get("test", ".bar");
    }

    @Test
    public void del() throws Exception {
        c._conn().flushDB();
        c.set("foobar", ".", new FooBarObject());
        c.del("foobar", ".foo");
    }

    @Test(expected = Exception.class)
    public void delException() throws Exception {
        c._conn().flushDB();
        c.set("foobar", ".", new FooBarObject());
        c.del("foobar", ".foo[1]");
    }

    @Test
    public void type() throws Exception {
        c._conn().flushDB();
        c.set("foobar", ".", new FooBarObject());
        assertSame(Object.class, c.type("foobar", "."));
        assertSame(String.class, c.type("foobar", ".foo"));
    }

    @Test(expected = Exception.class)
    public void typeException() throws Exception {
        c._conn().flushDB();
        c.set("foobar", ".", new FooBarObject());
        c.type("foobar", ".foo[1]");
    }

}