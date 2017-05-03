package io.rejson;

import com.google.gson.Gson;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
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
    private String host="52.7.3.128";
    private int port=6379;
    Jedis jedis  = new Jedis(host,port);

    @Before
    public void initialize() {
        g = new Gson();

    }

    @Test
    public void basicSetGetShouldSucceed() throws Exception {



        // naive set with a path
        JReJSON.set(jedis, "null", null, Path.RootPath());
        assertNull(JReJSON.get(jedis,"null", Path.RootPath()));

        // real scalar value and no path
        JReJSON.set(jedis,"str", "strong");
        assertEquals("strong", JReJSON.get(jedis,"str"));

        // A slightly more complex object
        IRLObject obj = new IRLObject();
        JReJSON.set(jedis,"obj", obj);
        Object expected = g.fromJson(g.toJson(obj), Object.class);
        assertTrue(expected.equals(JReJSON.get(jedis,"obj")));

        // check an update
        Path p = new Path(".str");
        JReJSON.set(jedis,"obj", "strung", p);
        assertEquals("strung", JReJSON.get(jedis,"obj", p));
    }

    @Test
    public void setExistingPathOnlyIfExistsShouldSucceed() throws Exception {
        jedis.flushDB();

        JReJSON.set(jedis,"obj", new IRLObject());
        Path p = new Path(".str");
        JReJSON.set(jedis,"obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
        assertEquals("strangle", JReJSON.get(jedis,"obj", p));
    }

    @Test
    public void setNonExistingOnlyIfNotExistsShouldSucceed() throws Exception {
        jedis.flushDB();

        JReJSON.set(jedis,"obj", new IRLObject());
        Path p = new Path(".none");
        JReJSON.set(jedis,"obj", "strangle", JReJSON.ExistenceModifier.NOT_EXISTS, p);
        assertEquals("strangle", JReJSON.get(jedis,"obj", p));
    }

    @Test(expected = Exception.class)
    public void setExistingPathOnlyIfNotExistsShouldFail() throws Exception {
        jedis.flushDB();

        JReJSON.set(jedis,"obj", new IRLObject());
        Path p = new Path(".str");
        JReJSON.set(jedis,"obj", "strangle", JReJSON.ExistenceModifier.NOT_EXISTS, p);
    }

    @Test(expected = Exception.class)
    public void setNonExistingPathOnlyIfExistsShouldFail() throws Exception {
        jedis.flushDB();

        JReJSON.set(jedis,"obj", new IRLObject());
        Path p = new Path(".none");
        JReJSON.set(jedis,"obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
    }

    @Test(expected = Exception.class)
    public void setException() throws Exception {
        jedis.flushDB();

        // should error on non root path for new key
        JReJSON.set(jedis,"test", "bar", new Path(".foo"));
    }

    @Test(expected = Exception.class)
    public void setMultiplePathsShouldFail() throws Exception {
        jedis.flushDB();
        JReJSON.set(jedis,"obj", new IRLObject());
        JReJSON.set(jedis,"obj", "strange", new Path(".str"), new Path(".str"));
    }

    @Test
    public void getMultiplePathsShouldSucceed() throws Exception {
        jedis.flushDB();

        // check multiple paths
        IRLObject obj = new IRLObject();
        JReJSON.set(jedis,"obj", obj);
        Object expected = g.fromJson(g.toJson(obj), Object.class);
        assertTrue(expected.equals(JReJSON.get(jedis,"obj", new Path("bTrue"), new Path("str"))));

    }

    @Test(expected = Exception.class)
    public void getException() throws Exception {
        jedis.flushDB();
        JReJSON.set(jedis,"test", "foo", Path.RootPath());
        JReJSON.get(jedis,"test", new Path(".bar"));
    }

    @Test
    public void delValidShouldSucceed() throws Exception {
        jedis.flushDB();

        // check deletion of a single path
        JReJSON.set(jedis,"obj", new IRLObject(), Path.RootPath());
        JReJSON.del(jedis,"obj", new Path(".str"));
        assertTrue(jedis.exists("obj"));

        // check deletion root using default root -> key is removed
        JReJSON.del(jedis,"obj");
        assertFalse(jedis.exists("obj"));
    }

    @Test(expected = Exception.class)
    public void delException() throws Exception {
        jedis.flushDB();
        JReJSON.set(jedis,"foobar", new FooBarObject(), Path.RootPath());
        JReJSON.del(jedis,"foobar", new Path(".foo[1]"));
    }

    @Test(expected = Exception.class)
    public void delMultiplePathsShoudFail() throws Exception {
        jedis.flushDB();
        JReJSON.del(jedis,"foobar", new Path(".foo"), new Path(".bar"));
    }

    @Test
    public void typeChecksShouldSucceed() throws Exception {
        jedis.flushDB();
        JReJSON.set(jedis,"foobar", new FooBarObject(), Path.RootPath());
        assertSame(Object.class, c.type(jedis,"foobar", Path.RootPath()));
        assertSame(String.class, c.type(jedis,"foobar", new Path(".foo")));
    }

    @Test(expected = Exception.class)
    public void typeException() throws Exception {
        jedis.flushDB();
        JReJSON.set(jedis,"foobar", new FooBarObject(), Path.RootPath());
        JReJSON.type(jedis, "foobar", new Path(".foo[1]"));
    }

    @Test(expected = Exception.class)
    public void type1Exception() throws Exception {
        jedis.flushDB();
        JReJSON.set(jedis,"foobar", new FooBarObject(), Path.RootPath());
        JReJSON.type(jedis,"foobar", new Path(".foo[1]"));

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