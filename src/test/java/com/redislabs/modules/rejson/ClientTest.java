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
import static org.junit.Assert.assertArrayEquals;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import redis.clients.jedis.Jedis;

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

    private static class Baz {
        private String quuz;
        private String grault;
        private String waldo;

        public Baz(final String quuz, final String grault, final String waldo) {
            this.quuz = quuz;
            this.grault = grault;
            this.waldo = waldo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null)
                return false;
            if (getClass() != o.getClass())
                return false;
            Baz other = (Baz) o;

            return Objects.equals(quuz, other.quuz) && //
                    Objects.equals(grault, other.grault) && //
                    Objects.equals(waldo, other.waldo);
        }
    }

    private static class Qux {
        private String quux;
        private String corge;
        private String garply;
        private Baz baz;

        public Qux(final String quux, final String corge, final String garply, final Baz baz) {
            this.quux = quux;
            this.corge = corge;
            this.garply = garply;
            this.baz = baz;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null)
                return false;
            if (getClass() != o.getClass())
                return false;
            Qux other = (Qux) o;

            return Objects.equals(quux, other.quux) && //
                    Objects.equals(corge, other.corge) && //
                    Objects.equals(garply, other.garply) && //
                    Objects.equals(baz, other.baz);
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
        Path p = Path.of(".str");
        client.set( "obj", "strung", p);
        assertEquals("strung", client.get( "obj", String.class, p));
    }

    @Test
    public void setExistingPathOnlyIfExistsShouldSucceed() throws Exception {
        client.set( "obj", new IRLObject());
        Path p = Path.of(".str");
        client.set( "obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
        assertEquals("strangle", client.get( "obj", String.class, p));
    }

    @Test
    public void setNonExistingOnlyIfNotExistsShouldSucceed() throws Exception {
        client.set( "obj", new IRLObject());
        Path p = Path.of(".none");
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
        Path p = Path.of(".str");
        client.set( "obj", "strangle", JReJSON.ExistenceModifier.NOT_EXISTS, p);
    }

    @Test(expected = Exception.class)
    public void setNonExistingPathOnlyIfExistsShouldFail() throws Exception {
        client.set( "obj", new IRLObject());
        Path p = Path.of(".none");
        client.set( "obj", "strangle", JReJSON.ExistenceModifier.MUST_EXIST, p);
    }

    @Test(expected = Exception.class)
    public void setException() throws Exception {
        // should error on non root path for new key
        client.set( "test", "bar", Path.of(".foo"));
    }

    @Test
    public void getMultiplePathsShouldSucceed() throws Exception {
        // check multiple paths
        IRLObject obj = new IRLObject();
        client.set( "obj", obj);
        Object expected = g.fromJson(g.toJson(obj), Object.class);
        assertTrue(expected.equals(client.get( "obj", Object.class, Path.of("bTrue"), Path.of("str"))));

    }

    @Test(expected = Exception.class)
    public void getException() throws Exception {
        client.set( "test", "foo", Path.ROOT_PATH);
        client.get( "test", String.class, Path.of(".bar"));
    }

    @Test
    public void delValidShouldSucceed() throws Exception {
        // check deletion of a single path
        client.set( "obj", new IRLObject(), Path.ROOT_PATH);
        client.del( "obj", Path.of(".str"));
        assertTrue(jedis.exists("obj"));

        // check deletion root using default root -> key is removed
        client.del( "obj");
        assertFalse(jedis.exists("obj"));
    }

    @org.junit.Ignore
    @Test
    public void delNonExistingPathsAreIgnored() throws Exception {
	    client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
	    client.del( "foobar", Path.of(".foo[1]")).longValue();
    }

    @Test
    public void typeChecksShouldSucceed() throws Exception {
        client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
        assertSame(Object.class, client.type( "foobar"));
        assertSame(Object.class, client.type( "foobar", Path.ROOT_PATH));
        assertSame(String.class, client.type( "foobar", Path.of(".foo")));
        assertSame(int.class, client.type( "foobar", Path.of(".fooI")));
        assertSame(float.class, client.type( "foobar", Path.of(".fooF")));
        assertSame(List.class, client.type( "foobar", Path.of(".fooArr")));
        assertSame(boolean.class, client.type( "foobar", Path.of(".fooB")));

        try {
          client.type( "foobar", Path.of(".fooErr"));
          fail();
        }catch(Exception e) {}
    }

    @Test(expected = Exception.class)
    public void typeException() throws Exception {
        client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
        client.type( "foobar", Path.of(".foo[1]"));
    }

    @Test(expected = Exception.class)
    public void type1Exception() throws Exception {
        client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
        client.type( "foobar", Path.of(".foo[1]"));
    }

    @Test
    public void testMultipleGetAtRootPathAllKeysExist() throws Exception {
        Baz baz1 = new Baz("quuz1", "grault1", "waldo1");
        Baz baz2 = new Baz("quuz2", "grault2", "waldo2");
        Qux qux1 = new Qux("quux1", "corge1", "garply1", baz1);
        Qux qux2 = new Qux("quux2", "corge2", "garply2", baz2);

        client.set("qux1", qux1);
        client.set("qux2", qux2);

        List<Qux> oneQux = client.mget(Qux.class, "qux1");
        List<Qux> allQux = client.mget(Qux.class, "qux1", "qux2");

        assertEquals(1, oneQux.size());
        assertEquals(2, allQux.size());

        assertEquals(qux1, oneQux.get(0));

        Qux testQux1 = allQux.stream() //
                .filter(q -> q.quux.equals("quux1")) //
                .findFirst() //
                .orElseThrow(() -> new NullPointerException(""));
        Qux testQux2 = allQux.stream() //
                .filter(q -> q.quux.equals("quux2")) //
                .findFirst() //
                .orElseThrow(() -> new NullPointerException(""));

        assertEquals(qux1, testQux1);
        assertEquals(qux2, testQux2);
    }

    @Test
    public void testMultipleGetAtRootPathWithMissingKeys() throws Exception {
        Baz baz1 = new Baz("quuz1", "grault1", "waldo1");
        Baz baz2 = new Baz("quuz2", "grault2", "waldo2");
        Qux qux1 = new Qux("quux1", "corge1", "garply1", baz1);
        Qux qux2 = new Qux("quux2", "corge2", "garply2", baz2);

        client.set("qux1", qux1);
        client.set("qux2", qux2);

        List<Qux> allQux = client.mget(Qux.class, "qux1", "qux2", "qux3");

        assertEquals(3, allQux.size());
        assertNull(allQux.get(2));
        allQux.removeAll(Collections.singleton(null));
        assertEquals(2, allQux.size());
    }

    @Test
    public void testMultipleGetWithPathPathAllKeysExist() throws Exception {
        Baz baz1 = new Baz("quuz1", "grault1", "waldo1");
        Baz baz2 = new Baz("quuz2", "grault2", "waldo2");
        Qux qux1 = new Qux("quux1", "corge1", "garply1", baz1);
        Qux qux2 = new Qux("quux2", "corge2", "garply2", baz2);

        client.set("qux1", qux1);
        client.set("qux2", qux2);

        List<Baz> allBaz = client.mget(Path.of("baz"), Baz.class, "qux1", "qux2");

        assertEquals(2, allBaz.size());

        Baz testBaz1 = allBaz.stream() //
                .filter(b -> b.quuz.equals("quuz1")) //
                .findFirst() //
                .orElseThrow(() -> new NullPointerException(""));
        Baz testBaz2 = allBaz.stream() //
                .filter(q -> q.quuz.equals("quuz2")) //
                .findFirst() //
                .orElseThrow(() -> new NullPointerException(""));

        assertEquals(baz1, testBaz1);
        assertEquals(baz2, testBaz2);
    }

    @Test
    public void testArrayLength() throws Exception {
      client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
      assertEquals(Long.valueOf(3L), client.arrLen( "foobar", Path.of(".fooArr")));
    }

    @Test
    public void testArrayAppendSameType() throws Exception {
      String json = "{ a: 'hello', b: [1, 2, 3], c: { d: ['ello'] }}";
      JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);    
        
      client.set( "test_arrappend", jsonObject, Path.ROOT_PATH);
      assertEquals(Long.valueOf(6L), client.arrAppend( "test_arrappend", Path.of(".b"), 4, 5, 6));
      
      Integer[] array = client.get("test_arrappend", Integer[].class, Path.of(".b"));
      assertArrayEquals(new Integer[] {1, 2, 3, 4, 5, 6}, array);
    }
    
    @Test
    public void testArrayAppendMultipleTypes() throws Exception {
      String json = "{ a: 'hello', b: [1, 2, 3], c: { d: ['ello'] }}";
      JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);    
        
      client.set( "test_arrappend", jsonObject, Path.ROOT_PATH);
      assertEquals(Long.valueOf(6L), client.arrAppend( "test_arrappend", Path.of(".b"), "foo", true, null));
      
      Object[] array = client.get("test_arrappend", Object[].class, Path.of(".b"));

      // NOTE: GSon converts numeric types to the most accommodating type (Double)
      // when type information is not provided (as in the Object[] below)
      assertArrayEquals(new Object[] {1.0, 2.0, 3.0, "foo", true, null}, array);
    }
    
    @Test
    public void testArrayAppendMultipleTypesWithDeepPath() throws Exception {
      String json = "{ a: 'hello', b: [1, 2, 3], c: { d: ['ello'] }}";
      JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);    
        
      client.set( "test_arrappend", jsonObject, Path.ROOT_PATH);
      assertEquals(Long.valueOf(4L), client.arrAppend( "test_arrappend", Path.of(".c.d"), "foo", true, null));
      
      Object[] array = client.get("test_arrappend", Object[].class, Path.of(".c.d"));
      assertArrayEquals(new Object[] {"ello", "foo", true, null}, array);
    }
    
    @Test
    public void testArrayAppendAgaintsEmptyArray() throws Exception {
      String json = "{ a: 'hello', b: [1, 2, 3], c: { d: [] }}";
      JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);  
        
      client.set( "test_arrappend", jsonObject, Path.ROOT_PATH);
      assertEquals(Long.valueOf(3L), client.arrAppend( "test_arrappend", Path.of(".c.d"), "a", "b", "c"));
      
      String[] array = client.get("test_arrappend", String[].class, Path.of(".c.d"));
      assertArrayEquals(new String[] {"a", "b", "c"}, array);
    }
    
    @Test(expected = Exception.class)
    public void testArrayAppendPathIsNotArray() throws Exception {
      String json = "{ a: 'hello', b: [1, 2, 3], c: { d: ['ello'] }}";
      JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);    
        
      client.set( "test_arrappend", jsonObject, Path.ROOT_PATH);
      client.arrAppend( "test_arrappend", Path.of(".a"), 1);
    }
    
    @Test
    public void testArrayIndexWithInts() throws Exception {
      client.set( "quxquux", new int[] {8,6,7,5,3,0,9}, Path.ROOT_PATH);
      assertEquals(Long.valueOf(2L), client.arrIndex( "quxquux", Path.ROOT_PATH, 7));
    }

    @Test
    public void testArrayIndexWithStrings() throws Exception {
      client.set( "quxquux", new String[] {"8","6","7","5","3","0","9"}, Path.ROOT_PATH);
      assertEquals(Long.valueOf(2L), client.arrIndex( "quxquux", Path.ROOT_PATH, "7"));
    }

    @Test
    public void testArrayIndexWithStringsAndPath() throws Exception {
      client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
      assertEquals(Long.valueOf(1L), client.arrIndex( "foobar", Path.of(".fooArr"), "b"));
    }
    
    @Test(expected = Exception.class)
    public void testArrayIndexNonExistentPath() throws Exception {
      client.set( "foobar", new FooBarObject(), Path.ROOT_PATH);
      assertEquals(Long.valueOf(1L), client.arrIndex( "foobar", Path.of(".barArr"), "x"));
    }
    
    @Test
    public void testArrayInsert() throws Exception {
      String json = "['hello', 'world', true, 1, 3, null, false]";
      JsonArray jsonArray = new Gson().fromJson(json, JsonArray.class);    
        
      client.set( "test_arrinsert", jsonArray, Path.ROOT_PATH);
      assertEquals(Long.valueOf(8L), client.arrInsert( "test_arrinsert", Path.ROOT_PATH, 1L, "foo"));
      
      Object[] array = client.get("test_arrinsert", Object[].class, Path.ROOT_PATH);

      // NOTE: GSon converts numeric types to the most accommodating type (Double)
      // when type information is not provided (as in the Object[] below)
      assertArrayEquals(new Object[] {"hello", "foo", "world", true, 1.0, 3.0, null, false}, array);
    }
    
    @Test
    public void testArrayInsertWithNegativeIndex() throws Exception {
      String json = "['hello', 'world', true, 1, 3, null, false]";
      JsonArray jsonArray = new Gson().fromJson(json, JsonArray.class);    
        
      client.set( "test_arrinsert", jsonArray, Path.ROOT_PATH);
      assertEquals(Long.valueOf(8L), client.arrInsert( "test_arrinsert", Path.ROOT_PATH, -1L, "foo"));
      
      Object[] array = client.get("test_arrinsert", Object[].class, Path.ROOT_PATH);
      assertArrayEquals(new Object[] {"hello", "world", true, 1.0, 3.0, null, "foo", false}, array);
    }
    
    @Test
    public void testArrayPop() throws Exception {
      client.set( "arr", new int[] {0,1,2,3,4}, Path.ROOT_PATH);
      assertEquals(Long.valueOf(4L), client.arrPop( "arr", Long.class, Path.ROOT_PATH, 4L));
      assertEquals(Long.valueOf(3L), client.arrPop( "arr", Long.class, Path.ROOT_PATH, -1L));
      assertEquals(Long.valueOf(2L), client.arrPop( "arr", Long.class));
      assertEquals(Long.valueOf(0L), client.arrPop( "arr", Long.class, Path.ROOT_PATH, 0L));
      assertEquals(Long.valueOf(1L), client.arrPop( "arr", Long.class));
    }
    
    @Test
    public void testArrayTrim() throws Exception {
      client.set( "arr", new int[] {0,1,2,3,4}, Path.ROOT_PATH);
      assertEquals(Long.valueOf(3L), client.arrTrim( "arr", Path.ROOT_PATH, 1L, 3L));
      
      Integer[] array = client.get("arr", Integer[].class, Path.ROOT_PATH);
      assertArrayEquals(new Integer[] {1, 2, 3}, array);
    }
    
    @Test
    public void testStringAppend() throws Exception {
      client.set( "str", "foo", Path.ROOT_PATH);
      assertEquals(Long.valueOf(6L), client.strAppend( "str", Path.ROOT_PATH, "bar"));
      assertEquals("foobar", client.get("str", String.class, Path.ROOT_PATH));
    }
    
    @Test
    public void testStringLen() throws Exception {
      client.set( "str", "foo", Path.ROOT_PATH);
      assertEquals(Long.valueOf(3L), client.strLen( "str", Path.ROOT_PATH));
    }
}
