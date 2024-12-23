package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.transform.sql.zeta.functions.StringFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ConcatWsFunctionTest {

    @Test
    public void testConcatWs() {
        Assertions.assertEquals("a;b", StringFunction.concatWs(genArgs1()));
        Assertions.assertEquals("a;b;c", StringFunction.concatWs(genArgs2()));
        Assertions.assertEquals("", StringFunction.concatWs(genArgs3()));
    }

    public List<Object> genArgs1() {
        List<Object> list = new ArrayList<>();
        String[] arr = new String[] {"a", "b"};
        String separator = ";";
        list.add(separator);
        list.add(arr);
        return list;
    }

    public List<Object> genArgs2() {
        List<Object> list = new ArrayList<>();
        String[] arr = new String[] {"a", "b"};
        String separator = ";";
        list.add(separator);
        list.add(arr);
        list.add("c");
        return list;
    }

    public List<Object> genArgs3() {
        List<Object> list = new ArrayList<>();
        String[] arr = new String[] {};
        String separator = ";";
        list.add(separator);
        list.add(arr);
        return list;
    }
}
