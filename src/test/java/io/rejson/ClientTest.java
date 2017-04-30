package io.rejson;

import com.google.gson.Gson;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import static junit.framework.TestCase.*;

class FooBarObject extends Object {
    public String foo;

    public FooBarObject() {
        this.foo = "bar";
    }
}

class IRLObject extends Object {
    public String str;
    public boolean bTrue;

    public IRLObject() {
        this.str = "string";
        this.bTrue = true;
    }
}


public class ClientTest {

    private JReJSON c;
    private Gson g;

    @Before
    public void initialize() {
        g = new Gson();
        c = new JReJSON("localhost", 6379);
    }

    @Test
    public void basicSetGetShouldSucceed() throws Exception {
        c._conn().flushDB();

        // naive set with a path
        c.set("null", null, Path.RootPath());
        assertNull(c.get("null", Path.RootPath()));

        // real scalar value and no path
        c.set("str", "strong");
        assertEquals("strong", c.get("str"));

        // A slightly more complex object
        IRLObject obj = new IRLObject();
        c.set("obj", obj);
        Object expected = g.fromJson(g.toJson(obj), Object.class);
        assertTrue(expected.equals(c.get("obj")));

        // check an update
        Path p = new Path(".str");
        c.set("obj", "strung", p);
        assertEquals("strung", c.get("obj", p));
    }

    @Test
    public void setExistingPathOnlyIfExistsShouldSucceed() throws Exception {
        c._conn().flushDB();

        c.set("obj", new IRLObject());
        Path p = new Path(".str");
        c.set("obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
        assertEquals("strangle", c.get("obj", p));
    }

    @Test
    public void setNonExistingOnlyIfNotExistsShouldSucceed() throws Exception {
        c._conn().flushDB();

        c.set("obj", new IRLObject());
        Path p = new Path(".none");
        c.set("obj", "strangle", JReJSON.ExistenceModifier.NOT_EXISTS, p);
        assertEquals("strangle", c.get("obj", p));
    }

    @Test(expected = Exception.class)
    public void setExistingPathOnlyIfNotExistsShouldFail() throws Exception {
        c._conn().flushDB();

        c.set("obj", new IRLObject());
        Path p = new Path(".str");
        c.set("obj", "strangle", JReJSON.ExistenceModifier.NOT_EXISTS, p);
    }

    @Test(expected = Exception.class)
    public void setNonExistingPathOnlyIfExistsShouldFail() throws Exception {
        c._conn().flushDB();

        c.set("obj", new IRLObject());
        Path p = new Path(".none");
        c.set("obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
    }

    @Test(expected = Exception.class)
    public void setException() throws Exception {
        c._conn().flushDB();

        // should error on non root path for new key
        c.set("test", "bar", new Path(".foo"));
    }

    @Test(expected = Exception.class)
    public void setMultiplePathsShouldFail() throws Exception {
        c._conn().flushDB();
        c.set("obj", new IRLObject());
        c.set("obj", "strange", new Path(".str"), new Path(".str"));
    }

    @Test
    public void getMultiplePathsShouldSucceed() throws Exception {
        c._conn().flushDB();

        // check multiple paths
        IRLObject obj = new IRLObject();
        c.set("obj", obj);
        Object expected = g.fromJson(g.toJson(obj), Object.class);
        assertTrue(expected.equals(c.get("obj", new Path("bTrue"), new Path("str"))));

    }

    @Test(expected = Exception.class)
    public void getException() throws Exception {
        c._conn().flushDB();
        c.set("test", "foo", Path.RootPath());
        c.get("test", new Path(".bar"));
    }

    @Test
    public void delValidShouldSucceed() throws Exception {
        c._conn().flushDB();

        // check deletion of a single path
        c.set("obj", new IRLObject(), Path.RootPath());
        c.del("obj", new Path(".str"));
        assertTrue(c._conn().exists("obj"));

        // check deletion root using default root -> key is removed
        c.del("obj");
        assertFalse(c._conn().exists("obj"));
    }

    @Test(expected = Exception.class)
    public void delException() throws Exception {
        c._conn().flushDB();
        c.set("foobar", new FooBarObject(), Path.RootPath());
        c.del("foobar", new Path(".foo[1]"));
    }

    @Test(expected = Exception.class)
    public void delMultiplePathsShoudFail() throws Exception {
        c._conn().flushDB();
        c.del("foobar", new Path(".foo"), new Path(".bar"));
    }

    @Test
    public void typeChecksShouldSucceed() throws Exception {
        c._conn().flushDB();
        c.set("foobar", new FooBarObject(), Path.RootPath());
        assertSame(Object.class, c.type("foobar", Path.RootPath()));
        assertSame(String.class, c.type("foobar", new Path(".foo")));
    }

    @Test(expected = Exception.class)
    public void typeException() throws Exception {
        c._conn().flushDB();
        c.set("foobar", new FooBarObject(), Path.RootPath());
        c.type("foobar", new Path(".foo[1]"));
    }

    @Test(expected = Exception.class)
    public void type1Exception() throws Exception {
        c._conn().flushDB();
        c.set("foobar", new FooBarObject(), Path.RootPath());
        c.type("foobar", new Path(".foo[1]"));

//        JedisPoolConfig conf = new JedisPoolConfig();
//        conf.setMaxTotal(55);
//        conf.setTestOnBorrow(false);
//        conf.setTestOnReturn(false);
//        conf.setTestOnCreate(false);
//        conf.setTestWhileIdle(false);
//        conf.setMinEvictableIdleTimeMillis(60000);
//        conf.setTimeBetweenEvictionRunsMillis(30000);
//        conf.setNumTestsPerEvictionRun(-1);
//        conf.setFairness(true);
//
//        JedisPool pool = new JedisPool(conf, "localhost", 6379,1000);
//
//        JReJSON jReJSON = (JReJSON) pool.getResource();




        //Pipeline pipe =  jReJSON.pipelined();


    }


}