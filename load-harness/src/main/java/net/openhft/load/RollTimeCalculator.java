package net.openhft.load;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public final class RollTimeCalculator {

    private static final long WINDOW_SIZE = TimeUnit.MINUTES.toMillis(15);

    public static void main(String[] args) {
       // System.out.println(getNextRollWindow());
    }

    public static LocalTime getNextRollWindow() {
        final long currentTimeMillis = System.currentTimeMillis();
        final long remainder = currentTimeMillis % WINDOW_SIZE;
        final Instant instant = Instant.ofEpochMilli((currentTimeMillis - remainder) + WINDOW_SIZE);
        return LocalDateTime.ofEpochSecond(instant.getEpochSecond(), 0, ZoneOffset.UTC).toLocalTime();
    }
}
