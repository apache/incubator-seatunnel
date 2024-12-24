package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.transform.sql.zeta.functions.StringFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConcatWsFunctionTest {

    @Test
    public void testConcatWs() {
        Assertions.assertEquals("", StringFunction.concatWs(genArgs(";", new String[] {})));
        Assertions.assertEquals("", StringFunction.concatWs(genArgs(null, new String[] {})));
        Assertions.assertEquals(
                "a;b", StringFunction.concatWs(genArgs(";", new String[] {"a", "b"})));
        Assertions.assertEquals(
                "a;b", StringFunction.concatWs(genArgs(";", new String[] {"a", null, "b"})));
        Assertions.assertEquals(
                "ab",
                StringFunction.concatWs(genArgs("", new String[] {null, "a", null, "b", null})));
        Assertions.assertEquals(
                "ab", StringFunction.concatWs(genArgs(null, new String[] {"a", "b", null})));
        Assertions.assertEquals(
                "a;b;c", StringFunction.concatWs(genArgs(";", new String[] {"a", "b"}, "c")));
        Assertions.assertEquals(
                "a;b", StringFunction.concatWs(genArgs(";", new String[] {"a", "b"}, null)));
        Assertions.assertEquals(
                "a;b;1;2",
                StringFunction.concatWs(
                        genArgs(";", new String[] {"a", "b"}, new String[] {"1", "2"})));
    }

    public List<Object> genArgs(String separator, String[] arr) {
        List<Object> list = new ArrayList<>();
        list.add(separator);
        list.add(arr);
        return list;
    }

    public List<Object> genArgs(String separator, Object... arr) {
        List<Object> list = new ArrayList<>();
        list.add(separator);
        Collections.addAll(list, arr);
        return list;
    }
}
