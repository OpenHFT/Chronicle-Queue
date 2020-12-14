package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.function.Function;

@Deprecated /* For removal in x.22, This class is now internal.QueueOffsetSpec instead */
public final class QueueOffsetSpec {

    private static final String TOKEN_DELIMITER = ";";
    private final Type type;
    private final String[] spec;
    private QueueOffsetSpec(final Type type, final String[] spec) {
        this.type = type;
        this.spec = spec;
    }

    public static QueueOffsetSpec ofEpoch(final long epoch) {
        return new QueueOffsetSpec(Type.EPOCH, new String[]{Long.toString(epoch)});
    }

    public static QueueOffsetSpec ofRollTime(final LocalTime time, final ZoneId zoneId) {
        return new QueueOffsetSpec(Type.ROLL_TIME, new String[]{time.toString(), zoneId.toString()});
    }

    public static QueueOffsetSpec ofNone() {
        return new QueueOffsetSpec(Type.NONE, new String[]{});
    }

    public static QueueOffsetSpec parse(final String definition) {
        final String[] tokens = definition.split(TOKEN_DELIMITER);
        final Type type = Type.valueOf(tokens[0]);
        switch (type) {
            case EPOCH:
                expectArgs(tokens, 2);
                return new QueueOffsetSpec(type, new String[]{tokens[1]});
            case ROLL_TIME:
                expectArgs(tokens, 3);
                return new QueueOffsetSpec(type, new String[]{tokens[1], tokens[2]});
            case NONE:
                expectArgs(tokens, 1);
                return new QueueOffsetSpec(type, new String[]{});
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    public static String formatEpochOffset(final long epochOffset) {
        return String.format("%s;%s", Type.EPOCH.name(), Long.toString(epochOffset));
    }

    public static String formatRollTime(final LocalTime time, final ZoneId zoneId) {
        return String.format("%s;%s;%s", Type.ROLL_TIME.name(), time.toString(), zoneId.toString());
    }

    public static String formatNone() {
        return Type.NONE.name();
    }

    private static ZoneId toZoneId(final String zoneId) {
        return ZoneId.of(zoneId);
    }

    private static LocalTime toLocalTime(final String timestamp) {
        return LocalTime.parse(timestamp);
    }

    private static void expectArgs(final String[] tokens, final int expectedLength) {
        if (tokens.length != expectedLength) {
            throw new IllegalArgumentException("Expected " + expectedLength + " tokens in " + Arrays.toString(tokens));
        }
    }

    public void apply(final SingleChronicleQueueBuilder builder) {
        switch (type) {
            case EPOCH:
                builder.epoch(Long.parseLong(spec[0]));
                break;
            case ROLL_TIME:
                builder.rollTime(toLocalTime(spec[0]), toZoneId(spec[1]));
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    public String format() {
        return type.name() + TOKEN_DELIMITER + type.argFormatter.apply(spec);
    }

    public void validate() {
        switch (type) {
            case EPOCH:
                Long.parseLong(spec[0]);
                break;
            case ROLL_TIME:
                toLocalTime(spec[0]);
                toZoneId(spec[1]);
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    public enum Type {
        EPOCH(args -> args[0]),
        ROLL_TIME(args -> args[0] + TOKEN_DELIMITER + args[1]),
        NONE(args -> "");

        private final Function<String[], String> argFormatter;

        Type(final Function<String[], String> argFormatter) {
            this.argFormatter = argFormatter;
        }
    }
}
