/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.locks.LockSupport;

/**
 * A utility class for sampling the stack traces of a target thread.
 * This class creates a background daemon thread which periodically
 * samples the stack trace of the specified thread and stores the
 * latest snapshot.
 */
public class StackSampler {
    @NotNull
    private final Thread sampler;

    private volatile Thread thread = null;
    private volatile StackTraceElement[] stack = null;

    /**
     * Constructs a new StackSampler and starts the background thread
     * responsible for sampling the stack trace.
     */
    @SuppressWarnings("this-escape")
    public StackSampler() {
        sampler = new Thread(this::sampling, "Thread sampler");
        sampler.setDaemon(true);
        sampler.start();
    }

    /**
     * Continuously samples the stack trace of the target thread at
     * periodic intervals. This method is internally used by the background
     * thread created in the constructor.
     */
    void sampling() {
        while (!Thread.currentThread().isInterrupted()) {
            Thread t = thread;
            if (t != null) {
                StackTraceElement[] stack0 = t.getStackTrace();
                if (thread == t)
                    stack = stack0;
            }
            LockSupport.parkNanos(10_000);
        }
    }

    /**
     * Stops the stack sampling by interrupting the background thread.
     */
    public void stop() {
        sampler.interrupt();
    }

    /**
     * Sets the thread to be sampled.
     *
     * @param thread the target thread whose stack trace should be sampled.
     */
    public void thread(Thread thread) {
        this.thread = thread;
    }

    /**
     * Retrieves the latest sampled stack trace and resets the internal
     * state for subsequent sampling.
     *
     * @return the latest stack trace sampled or null if no stack trace was sampled.
     */
    @Nullable
    public StackTraceElement[] getAndReset() {
        final StackTraceElement[] lStack = this.stack;
        thread = null;
        this.stack = null;
        return lStack;
    }
}
