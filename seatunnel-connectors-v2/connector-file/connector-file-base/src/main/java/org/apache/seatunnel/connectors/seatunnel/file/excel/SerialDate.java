package org.apache.seatunnel.connectors.seatunnel.file.excel;

public class SerialDate {

    // days from 1899-12-31 to Instant.EPOCH (1970-01-01T00:00:00Z)
    public static final long EPOCH = -25568L;

    private long serialDays;
    private double serialTime;
    private long epochDays;
    private long daySeconds;

    /** @param date number of Excel-days since <i>January 0, 1899</i> */
    public SerialDate(long date) {
        serialDays = date;
        if (date > 59) --date;
        epochDays = EPOCH + date;
    }

    /** @param date number of days since <i>January 0, 1899</i> with a time fraction */
    public SerialDate(double date) {
        this((long) date);
        serialTime = date - serialDays;
        daySeconds = Math.round(serialTime * 24 * 60 * 60);
    }

    /** @return days since 1970-01-01 */
    public long toEpochDays() {
        return epochDays;
    }

    /** @return seconds of the day for this SerialDate */
    public long toDaySeconds() {
        return daySeconds;
    }

    /** @return a value suitable for an Excel date */
    public double getSerialDate() {
        return serialTime + serialDays;
    }
}
