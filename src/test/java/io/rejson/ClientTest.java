package io.rejson;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.*;

public class ClientTest {

    private Client c;

    @Before
    public void initialize() {
        c = new Client("localhost", 6379);
    }

    @Test
    public void set() throws Exception {
        c._conn().flushDB();

        // basic set  - should succeed
        c.set("test", ".", null);
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
}