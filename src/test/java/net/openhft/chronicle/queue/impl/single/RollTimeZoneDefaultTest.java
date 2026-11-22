package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;
import java.time.LocalTime;
import java.time.ZoneId;
import static org.junit.Assert.*;

public class RollTimeZoneDefaultTest {
    private static final ZoneId ZONE = ZoneId.of("Europe/Paris");

    @Test public void settingZoneAloneDefaultsToMidnight() {
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("unused").rollTimeZone(ZONE);
        assertEquals(ZONE, builder.rollTimeZone());
        assertEquals(0, builder.epoch());
    }

    @Test public void zoneBeforeTimeHasSameEffectAsTimeBeforeZone() {
        LocalTime time = LocalTime.of(6, 30);
        SingleChronicleQueueBuilder zoneFirst = SingleChronicleQueueBuilder.binary("unused").rollTimeZone(ZONE).rollTime(time);
        SingleChronicleQueueBuilder timeFirst = SingleChronicleQueueBuilder.binary("unused").rollTime(time).rollTimeZone(ZONE);
        assertEquals(timeFirst.rollTimeZone(), zoneFirst.rollTimeZone());
        assertEquals(timeFirst.epoch(), zoneFirst.epoch());
    }

    @Test public void changingZoneKeepsExplicitRollTime() {
        LocalTime time = LocalTime.of(8, 15);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("unused").rollTime(time).rollTimeZone(ZONE);
        assertEquals(ZONE, builder.rollTimeZone());
        assertEquals(time.toSecondOfDay() * 1000L, builder.epoch());
    }
}
