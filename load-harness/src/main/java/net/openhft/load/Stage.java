package net.openhft.load;

import net.openhft.load.messages.EightyByteMessage;

import java.util.concurrent.TimeUnit;

public final class Stage implements MethodDefinition {
    private static final long READ_LAG_REPORT_INTERVAL_NS = TimeUnit.SECONDS.toNanos(10L);
    private final MethodDefinition output;
    private final int index;
    private final long mask;
    private long lastLagReport = 0L;

    public Stage(final MethodDefinition output, final int index) {
        if (index > 6) {
            throw new IllegalArgumentException("Too many stages");
        }
        this.output = output;
        this.index = index;
        this.mask = 1 << index;
    }

    @Override
    public void onEightyByteMessage(final EightyByteMessage message) {
        final long currentNanoTime = System.nanoTime();
        if (lastLagReport == 0L) {
            lastLagReport = currentNanoTime;
        }

        if (currentNanoTime > lastLagReport + READ_LAG_REPORT_INTERVAL_NS) {
            System.out.printf("Stage %d is currently %dms behind publisher%n",
                    index, TimeUnit.NANOSECONDS.toMillis(currentNanoTime - message.publishNanos));
            lastLagReport = currentNanoTime;
        }

        switch (index) {
            case 0:
                message.t0 = currentNanoTime;
                break;
            case 1:
                message.t1 = currentNanoTime;
                break;
            case 2:
                message.t2 = currentNanoTime;
                break;
            case 3:
                message.t3 = currentNanoTime;
                break;
            case 4:
                message.t4 = currentNanoTime;
                break;
            case 5:
                message.t5 = currentNanoTime;
                break;
            case 6:
                message.t6 = currentNanoTime;
                break;
        }

        if ((message.stagesToPublishBitMask & mask) != 0) {
            output.onEightyByteMessage(message);
        }
    }
}