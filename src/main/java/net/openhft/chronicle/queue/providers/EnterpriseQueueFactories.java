package net.openhft.chronicle.queue.providers;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueFactory;
import org.jetbrains.annotations.NotNull;

import java.util.ServiceLoader;

public class EnterpriseQueueFactories {

    private static final String PREFERRED_FACTORY_CLASS_NAME = Jvm.getProperty("net.openhft.chronicle.queue.providers.EnterpriseQueueWrapper",
            "software.chronicle.enterprise.queue.EnterpriseQueueFactory");

    private static QueueFactory queueFactory;

    /**
     * Get the {@link QueueFactory}
     *
     * @return the active queue wrapper
     */
    @NotNull
    public static QueueFactory get() {
        if (queueFactory == null) {
            final ServiceLoader<QueueFactory> load = ServiceLoader.load(QueueFactory.class);
            for (QueueFactory factory : load) {
                // last one in wins, unless we encounter the "preferred" one
                queueFactory = factory;
                if (PREFERRED_FACTORY_CLASS_NAME.equals(factory.getClass().getName())) {
                    break;
                }
            }
            if (queueFactory == null) {
                Jvm.error().on(EnterpriseQueueFactories.class, "There's no queue wrapper factory configured, this shouldn't happen. ");
                queueFactory = new SingleChronicleQueueFactory();
            }
        }
        return queueFactory;
    }
}
