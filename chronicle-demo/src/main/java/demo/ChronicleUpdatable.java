package demo;

import java.util.List;

/**
 * Created by daniel on 19/06/2014.
 */
public interface ChronicleUpdatable {
    public void messageProduced();

    public void setFileNames(List<String> files);

    public void addTimeMillis(long l);

    public void messageRead();

    public void tcpMessageRead();
}
