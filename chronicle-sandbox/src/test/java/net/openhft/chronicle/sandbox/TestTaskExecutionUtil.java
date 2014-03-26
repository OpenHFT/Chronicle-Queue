package net.openhft.chronicle.sandbox;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Utility methods for executing tests using threads.
 */
public final class TestTaskExecutionUtil {

    /**
     * Ensure no instances of this utility class.
     */
    private TestTaskExecutionUtil() {
    }

    /**
     * Execute specified tasks in independent threads.
     */
    public static void executeConcurrentTasks(final List<? extends Callable<Void>> tasks, final long taskTimeoutMillis) {

        // Create and start a thread per task
        final List<TaskRunner> taskRunners = new ArrayList<TaskRunner>();
        final List<Thread> threads = new ArrayList<Thread>();
        for (Callable<Void> task : tasks) {
            final TaskRunner taskRunner = new TaskRunner(task);
            taskRunners.add(taskRunner);
            final Thread thread = new Thread(taskRunner, task.toString());
            threads.add(thread);

            thread.start();
        }

        // Wait for all tasks to finish
        try {
            for (Thread thread : threads) {
                thread.join(taskTimeoutMillis);
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }

        // Fail if any tasks failed
        for (TaskRunner taskRunner : taskRunners) {
            taskRunner.assertIfFailed();
        }
    }

    private static class TaskRunner implements Runnable {
        private final Callable<?> task;
        private volatile AssertionError failure;

        public TaskRunner(final Callable<?> task) {
            this.task = task;
        }

        @Override
        public void run() {
            try {
                task.call();
            } catch (Throwable e) {
                failure = new AssertionError("Task failed", e);
            }
        }

        public void assertIfFailed() {
            if (failure != null) {
                throw failure;
            }
        }
    }
}
