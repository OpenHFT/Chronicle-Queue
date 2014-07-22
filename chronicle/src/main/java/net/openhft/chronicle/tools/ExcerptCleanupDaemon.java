/*
 * Copyright 2014 Higher Frequency Trading
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.tools;

import net.openhft.chronicle.ExcerptCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.ReferenceQueue;
import java.util.LinkedList;

public class ExcerptCleanupDaemon {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExcerptCleanupDaemon.class);

    private static ExcerptCleanupDaemon DAEMON = null;

    private final ReferenceQueue<ExcerptCommon> queue;
    private final LinkedList<ExcerptPhantomReference> refs;
    private final Thread thread;

    public static synchronized ExcerptCleanupDaemon get() {
        return (DAEMON != null)
            ? DAEMON
            : (DAEMON = new ExcerptCleanupDaemon());
    }

    public void track(final ExcerptCommon excerpt) {
        synchronized (refs) {
            refs.add(new ExcerptPhantomReference(excerpt, queue));
        }
    }

    private ExcerptCleanupDaemon() {
        queue = new ReferenceQueue<ExcerptCommon>();
        refs  = new LinkedList<ExcerptPhantomReference>();

        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        ExcerptPhantomReference ref = (ExcerptPhantomReference)queue.remove() ;
                        LOGGER.info("Cleaning up");
                        ref.cleanup() ;

                        synchronized (refs) {
                            refs.remove(ref);
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Exception",e);
                    }
                }
            }
        });

        thread.setName(ExcerptCleanupDaemon.class.getName());
        thread.setPriority(Thread.MIN_PRIORITY);
        thread.setDaemon(true);
        thread.start();
    }
}
