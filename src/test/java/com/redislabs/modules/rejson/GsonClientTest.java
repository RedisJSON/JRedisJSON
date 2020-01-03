package com.redislabs.modules.rejson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Name : GsonClientTest
 * <p>
 * Description : Passing Specific Gson
 * <p>
 * Date : 15/10/2019
 * <p>
 * Create by : Mohammed ElAdly
 */
public class GsonClientTest {

    static JReJSON client;
    static Gson gson;

    @BeforeClass
    public static void setUp() {


        GsonBuilder gsonBuilder = new GsonBuilder();

        gson = gsonBuilder.create();

        client = new JReJSON("localhost", 7001, gson);
    }

    @Test
    public void passSpecificGson() {

        client.set("personObj", new Person());

        Object personObj = client.get("personObj");
        Person personObj1 = client.get("personObj", Person.class);

        System.out.println("Object:" + personObj);

        String personObj2 = client.get("personObj", new Path(".name")).toString();
        System.out.println(personObj2);

        System.out.println("Casted Object: " + personObj1);


    }

    private static class Person {
        public String name;
        public int age;

        public Person() {
            this.name = "Mohammed ElAdly";
            this.age = 27;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}