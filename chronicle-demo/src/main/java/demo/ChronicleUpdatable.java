package demo;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by daniel on 19/06/2014.
 */
public interface ChronicleUpdatable {
    public void setFileNames(List<String> files);

    public void addTimeMillis(long l);

    public void incrMessageRead();

    public void incrTcpMessageRead();

    AtomicLong tcpMessageRead();

    AtomicLong count1();

    AtomicLong count2();
}
