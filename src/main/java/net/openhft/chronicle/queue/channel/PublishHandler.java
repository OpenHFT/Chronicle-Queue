/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.channel;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.channel.impl.PublishQueueChannel;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.channel.*;

import static net.openhft.chronicle.queue.channel.PipeHandler.newQueue;

/**
 * The {@code PublishHandler} class manages the process of publishing data from a Chronicle channel to a Chronicle queue.
 * It reads messages from the channel and writes them to a queue, ensuring data is synchronized based on the provided
 * {@link SyncMode}.
 */
@SuppressWarnings("deprecation")
public class PublishHandler extends AbstractHandler<PublishHandler> {

    // Queue to publish messages to
    private String publish;

    // Synchronization mode for the queue
    private SyncMode syncMode;

    // Source ID for the publish queue
    private int publishSourceId = 0;

    /**
     * Copies data from a {@link ChronicleChannel} to a {@link ChronicleQueue} using the provided {@link Pauser} to manage delays.
     * It writes to the queue via the {@link ExcerptAppender} and handles synchronization as per the {@code SyncMode}.
     *
     * @param channel      The channel to read data from
     * @param pauser       The pauser used to manage pauses during reading
     * @param publishQueue The ChronicleQueue to write data to
     * @param syncMode     The mode used to control when the data is synchronized to disk
     */
    static void copyFromChannelToQueue(ChronicleChannel channel, Pauser pauser, ChronicleQueue publishQueue, SyncMode syncMode) {
        try (ChronicleQueue publishQ = publishQueue;
             ExcerptAppender appender = publishQ.createAppender()) {

            // Assuming thread safety is handled by external mechanisms
            appender.singleThreadedCheckDisabled(true);

            boolean needsSync = false;

            // Loop until the channel is closed
            while (!channel.isClosed()) {
                try (DocumentContext dc = channel.readingDocument()) {
                    pauser.unpause();

                    // No new data available
                    if (!dc.isPresent()) {
                        if (needsSync) {
                            syncAppender(appender, syncMode);
                            needsSync = false;
                        }
                        continue;
                    }

                    // Handle metadata messages
                    if (dc.isMetaData()) {
                        // read message
                        continue;
                    }

                    // Copy the content from the channel to the queue
                    try (DocumentContext dc2 = appender.writingDocument()) {
                        dc.wire().copyTo(dc2.wire());
                        needsSync = syncMode == SyncMode.SYNC || syncMode == SyncMode.ASYNC;
                    }
                }
            }

        } finally {
            // Reset the thread name after processing
            Thread.currentThread().setName("connections");
        }
    }

    /**
     * Synchronizes the appender if the synchronization mode is {@link SyncMode#SYNC}.
     *
     * @param appender  The appender used to write to the queue
     * @param syncMode  The synchronization mode
     */
    private static void syncAppender(ExcerptAppender appender, SyncMode syncMode) {
        if (syncMode == SyncMode.SYNC) {
            try (DocumentContext dc2 = appender.writingDocument()) {
                dc2.wire().write("sync").text("");
            }
        }
        appender.sync();
    }

    // Getter and setter for the publish queue name

    public String publish() {
        return publish;
    }

    public PublishHandler publish(String publish) {
        this.publish = publish;
        return this;
    }

    // Getter and setter for the sync mode

    public SyncMode syncMode() {
        return syncMode;
    }

    public PublishHandler syncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    // Getter and setter for the publish source ID

    public int publishSourceId() {
        return publishSourceId;
    }

    public PublishHandler publishSourceId(int publishSourceId) {
        this.publishSourceId = publishSourceId;
        return this;
    }

    /**
     * Executes the {@code PublishHandler} by creating a publishing pipeline between the ChronicleChannel and the ChronicleQueue.
     *
     * @param context The context for managing resources and locks
     * @param channel The Chronicle channel to read data from
     */
    @SuppressWarnings("try")
    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
        Pauser pauser = Pauser.balanced();

        // Set the current thread name for identification purposes
        Thread.currentThread().setName("publish~reader");

        try (AffinityLock lock = context.affinityLock()) {
            // Copy data from the channel to the queue
            copyFromChannelToQueue(channel, pauser, newQueue(context, publish, syncMode, publishSourceId), syncMode);
        }
    }

    /**
     * Creates an internal {@link ChronicleChannel} using the provided configuration and context.
     *
     * @param context    The Chronicle context for managing resources
     * @param channelCfg The configuration for the Chronicle channel
     * @return A new {@link PublishQueueChannel} instance for internal channel communication
     */
    @Override
    public ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg<?> channelCfg) {
        return new PublishQueueChannel(channelCfg, this, newQueue(context, publish, syncMode, publishSourceId));
    }
}
