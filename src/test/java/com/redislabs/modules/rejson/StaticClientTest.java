/*
 * BSD 2-Clause License
 *
 * Copyright (c) 2017, Redis Labs
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.redislabs.modules.rejson;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;

public class StaticClientTest {

    /* A simple class that represents an object in real life */
	@SuppressWarnings("unused")
    private static class IRLObject {
        public String str;
        public boolean bTrue;

        public IRLObject() {
            this.str = "string";
            this.bTrue = true;
        }
    }

	@SuppressWarnings("unused")
    private static class FooBarObject {
        public String foo;

        public FooBarObject() {
            this.foo = "bar";
        }
    }
    
    private static class HM {
      public HashMap<String, String> v = new HashMap<>();
    
      public void set(String k, String v) {
        this.v.put(k, v);
      }
      
      public String get(String k) {
        return this.v.get(k);
      }
    }

    private Gson g;
    private String host="localhost";
    private int port=6379;
    final Jedis jedis = new Jedis(host,port);
    final JReJSON reJSON = new JReJSON(host,port);

    @Before
    public void initialize() {
        g = new Gson();
    }

    @Test
    public void basicSetGetShouldSucceed() throws Exception {

        // naive set with a path
        reJSON.set("null", null, Path.ROOT_PATH);
        assertNull(reJSON.get("null", String.class, Path.ROOT_PATH));

        // real scalar value and no path
        reJSON.set("str", "strong");
        assertEquals("strong", reJSON.get("str"));

        // a slightly more complex object
        IRLObject obj = new IRLObject();
        reJSON.set("obj", obj);
        Object expected = g.fromJson(g.toJson(obj), Object.class);
        assertTrue(expected.equals(reJSON.get("obj")));

        // check an update
        Path p = new Path(".str");
        reJSON.set("obj", "strung", p);
        assertEquals("strung", reJSON.get("obj", String.class, p));
    }

    @Test
    public void setExistingPathOnlyIfExistsShouldSucceed() throws Exception {
        jedis.flushDB();

        reJSON.set("obj", new IRLObject());
        Path p = new Path(".str");
        reJSON.set("obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
        assertEquals("strangle", reJSON.get("obj", String.class, p));
    }

    @Test
    public void setNonExistingOnlyIfNotExistsShouldSucceed() throws Exception {
        jedis.flushDB();

        reJSON.set("obj", new IRLObject());
        Path p = new Path(".none");
        reJSON.set("obj", "strangle", JReJSON.ExistenceModifier.NOT_EXISTS, p);
        assertEquals("strangle", reJSON.get("obj", String.class, p));
    }

    @Test(expected = Exception.class)
    public void setExistingPathOnlyIfNotExistsShouldFail() throws Exception {
        jedis.flushDB();

        reJSON.set("obj", new IRLObject());
        Path p = new Path(".str");
        reJSON.set("obj", "strangle", JReJSON.ExistenceModifier.NOT_EXISTS, p);
    }

    @Test(expected = Exception.class)
    public void setNonExistingPathOnlyIfExistsShouldFail() throws Exception {
        jedis.flushDB();

        reJSON.set("obj", new IRLObject());
        Path p = new Path(".none");
        reJSON.set("obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
    }

    @Test(expected = Exception.class)
    public void setException() throws Exception {
        jedis.flushDB();

        // should error on non root path for new key
        reJSON.set("test", "bar", new Path(".foo"));
    }

    @SuppressWarnings("deprecation")
    @Test(expected = Exception.class)
    public void setMultiplePathsShouldFail() throws Exception {
        jedis.flushDB();
        reJSON.set("obj", new IRLObject());
        JReJSON.set(jedis, "obj", "strange", new Path(".str"), new Path(".str"));
    }

    @Test
    public void getMultiplePathsShouldSucceed() throws Exception {
        jedis.flushDB();

        // check multiple paths
        IRLObject obj = new IRLObject();
        reJSON.set("obj", obj);
        Object expected = g.fromJson(g.toJson(obj), Object.class);
        assertTrue(expected.equals(reJSON.get("obj", Object.class, new Path("bTrue"), new Path("str"))));

    }
        
    /**
     * https://github.com/RedisJSON/JRedisJSON/issues/16
     */
    @Test
    public void objectGeneration() throws Exception {
        jedis.flushDB();

        HM articleMapOne = new HM();
        articleMapOne.set("ar01", "Intro to Map");
        articleMapOne.set("ar02", "Some article");
        reJSON.set("key", articleMapOne);
        HM dest = reJSON.<HM>get("key", HM.class, Path.ROOT_PATH);
        assertEquals("Intro to Map", dest.get("ar01"));
        assertEquals("Some article", dest.get("ar02"));
    }

    @Test(expected = Exception.class)
    public void getException() throws Exception {
        jedis.flushDB();
        reJSON.set("test", "foo", Path.ROOT_PATH);
        reJSON.get("test", String.class, new Path(".bar"));
    }

    @Test
    public void delValidShouldSucceed() throws Exception {
        jedis.flushDB();

        // check deletion of a single path
        reJSON.set("obj", new IRLObject(), Path.ROOT_PATH);
        reJSON.del("obj", new Path(".str"));
        assertTrue(jedis.exists("obj"));

        // check deletion root using default root -> key is removed
        reJSON.del("obj");
        assertFalse(jedis.exists("obj"));
    }

    public void delException() throws Exception {
        jedis.flushDB();
        reJSON.set("foobar", new FooBarObject(), Path.ROOT_PATH);
        assertEquals(0L, reJSON.del( "foobar", new Path(".foo[1]")).longValue());
    }

    @SuppressWarnings("deprecation")
    @Test(expected = Exception.class)
    public void delMultiplePathsShoudFail() throws Exception {
        jedis.flushDB();
        JReJSON.del(jedis, "foobar", new Path(".foo"), new Path(".bar"));
    }

    @Test
    public void typeChecksShouldSucceed() throws Exception {
        jedis.flushDB();
        reJSON.set("foobar", new FooBarObject(), Path.ROOT_PATH);
        assertSame(Object.class, reJSON.type("foobar", Path.ROOT_PATH));
        assertSame(String.class, reJSON.type("foobar", new Path(".foo")));
    }

    @Test(expected = Exception.class)
    public void typeException() throws Exception {
        jedis.flushDB();
        reJSON.set("foobar", new FooBarObject(), Path.ROOT_PATH);
        reJSON.type("foobar", new Path(".foo[1]"));
    }

    @Test(expected = Exception.class)
    public void type1Exception() throws Exception {
        jedis.flushDB();
        reJSON.set("foobar", new FooBarObject(), Path.ROOT_PATH);
        reJSON.type("foobar", new Path(".foo[1]"));
    }
}
