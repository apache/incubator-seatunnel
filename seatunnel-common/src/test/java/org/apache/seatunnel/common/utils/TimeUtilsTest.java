package org.apache.seatunnel.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimeUtilsTest {
    @Test
    public void testMatchTimeFormatter() {
        String timeStr = "12:12:12";
        Assertions.assertEquals(
                "12:12:12",
                TimeUtils.parse(timeStr, TimeUtils.matchTimeFormatter(timeStr)).toString());

        timeStr = "12:12:12.123";
        Assertions.assertEquals(
                "12:12:12.123",
                TimeUtils.parse(timeStr, TimeUtils.matchTimeFormatter(timeStr)).toString());
    }
}
