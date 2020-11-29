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
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertThrows;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

public class ClientTest {

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
        public boolean fooB;
        public int fooI;
        public float fooF;
        public String[] fooArr;

        public FooBarObject() {
            this.foo = "bar";
            this.fooB = true;
            this.fooI = 6574;
            this.fooF = 435.345f;
            this.fooArr = new String[]{"a", "b","c"};
        }
    }

    private final Gson g = new Gson();
    private final JReJSON client = new JReJSON("localhost",6379);
    private final Jedis jedis = new Jedis("localhost",6379);

    @Before
    public void cleanup() {
        jedis.flushDB();
    }

    @Test
    public void noArgsConstructorReturnsClientToLocalMachine() {
    	final JReJSON defaultClient = new JReJSON();
    	final JReJSON explicitLocalClient = new JReJSON("localhost", 6379);

        // naive set with a path
    	defaultClient.set("null", null, Path.ROOT_PATH);
        assertNull(explicitLocalClient.get("null", String.class, Path.ROOT_PATH));
    }

    @Test
    public void basicSetGetShouldSucceed() throws Exception {

        // naive set with a path
    	client.set("null", null, Path.ROOT_PATH);
        assertNull(client.get("null", String.class, Path.ROOT_PATH));

        // real scalar value and no path
        client.set( "str", "strong");
        assertEquals("strong", client.get( "str"));

        // a slightly more complex object
        IRLObject obj = new IRLObject();
        client.set( "obj", obj);
        Object expected = g.fromJson(g.toJson(obj), Object.class);
        assertTrue(expected.equals(client.get( "obj")));

        // check an update
        Path p = new Path(".str");
        client.set( "obj", "strung", p);
        assertEquals("strung", client.get( "obj", String.class, p));
    }

    @Test
    public void setExistingPathOnlyIfExistsShouldSucceed() throws Exception {
        client.set( "obj", new IRLObject());
        Path p = new Path(".str");
        client.set( "obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
        assertEquals("strangle", client.get( "obj", String.class, p));
    }

    @Test
    public void setNonExistingOnlyIfNotExistsShouldSucceed() throws Exception {
        client.set( "obj", new IRLObject());
        Path p = new Path(".none");
        client.set( "obj", "strangle", JReJSON.ExistenceModifier.NOT_EXISTS, p);
        assertEquals("strangle", client.get( "obj", String.class, p));
    }

    @Test
    public void setWithoutAPathDefaultsToRootPath() throws Exception {
        client.set( "obj1", new IRLObject());
        client.set( "obj1", "strangle", JReJSON.ExistenceModifier.MUST_EXIST);
        assertEquals("strangle", client.get( "obj1", String.class, Path.ROOT_PATH));
    }

    @Test(expected = Exception.class)
    public void setExistingPathOnlyIfNotExistsShouldFail() throws Exception {
        client.set( "obj", new IRLObject());
        Path p = new Path(".str");
        client.set( "obj", "strangle", JReJSON.ExistenceModifier.NOT_EXISTS, p);
    }

    @Test(expected = Exception.class)
    public void setNonExistingPathOnlyIfExistsShouldFail() throws Exception {
        client.set( "obj", new IRLObject());
        Path p = new Path(".none");
        client.set( "obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
    }

    @Test(expected = Exception.class)
    public void setException() throws Exception {
        // should error on non root path for new key
        client.set( "test", "bar", new Path(".foo"));
    }

    @Test
    public void getMultiplePathsShouldSucceed() throws Exception {
        // check multiple paths
        IRLObject obj = new IRLObject();
        client.set( "obj", obj);
        Object expected = g.fromJson(g.toJson(obj), Object.class);
        assertTrue(expected.equals(client.get( "obj", Object.class, new Path("bTrue"), new Path("str"))));

    }

    @Test(expected = Exception.class)
    public void getException() throws Exception {
        client.set( "test", "foo", Path.ROOT_PATH);
        client.get( "test", String.class, new Path(".bar"));
    }

    @Test
    public void delValidShouldSucceed() throws Exception {
        // check deletion of a single path
        client.set( "obj", new IRLObject(), Path.ROOT_PATH);
        client.del( "obj", new Path(".str"));
        assertTrue(jedis.exists("obj"));

        // check deletion root using default root -> key is removed
        client.del( "obj");
        assertFalse(jedis.exists("obj"));
    }

    @Test
    public void delException() throws Exception {
    	Exception ex = assertThrows(JedisDataException.class, () -> {
    	        client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
    	        client.del( "foobar", new Path(".foo[1]")).longValue();
    	});

    	assertTrue(ex.getMessage().contains("ERR invalid index '[1]' at level 1 in path"));
    }

    @Test
    public void typeChecksShouldSucceed() throws Exception {
        client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
        assertSame(Object.class, client.type( "foobar"));
        assertSame(Object.class, client.type( "foobar", Path.ROOT_PATH));
        assertSame(String.class, client.type( "foobar", new Path(".foo")));
        assertSame(int.class, client.type( "foobar", new Path(".fooI")));
        assertSame(float.class, client.type( "foobar", new Path(".fooF")));
        assertSame(List.class, client.type( "foobar", new Path(".fooArr")));
        assertSame(boolean.class, client.type( "foobar", new Path(".fooB")));

        try {
          client.type( "foobar", new Path(".fooErr"));
          fail();
        }catch(Exception e) {}
    }

    @Test(expected = Exception.class)
    public void typeException() throws Exception {
        client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
        client.type( "foobar", new Path(".foo[1]"));
    }

    @Test(expected = Exception.class)
    public void type1Exception() throws Exception {
        client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
        client.type( "foobar", new Path(".foo[1]"));
	}

}
