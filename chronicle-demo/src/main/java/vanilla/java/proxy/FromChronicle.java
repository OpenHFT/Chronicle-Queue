package vanilla.java.proxy;

import net.openhft.chronicle.ExcerptTailer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class FromChronicle<T> {
    private final T instance;
    private final ExcerptTailer tailer;

    public FromChronicle(T instance, ExcerptTailer tailer) {
        this.instance = instance;
        this.tailer = tailer;
    }

    public void readOne() {
        if (tailer.nextIndex()) {
            Method m = (Method) tailer.readObject();
            Object[] args = null;
            int len = (int) tailer.readStopBit();
            if (len > 0) {
                for (int i = 0; i < len; i++)
                    args[i] = tailer.readObject();
            }
            try {
                m.invoke(instance, args);
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }
}
