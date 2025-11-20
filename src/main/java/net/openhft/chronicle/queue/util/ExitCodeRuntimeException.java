package net.openhft.chronicle.queue.util;

import org.jetbrains.annotations.Nullable;

public class ExitCodeRuntimeException extends RuntimeException{
    private final int exitCode;

    /**
     * Constructs a new ExitCodeRuntimeException with the specified exit code and message.
     *
     * @param exitCode the exit code associated with this exception
     * @param message  the detail message
     */
    public ExitCodeRuntimeException(int exitCode, String message) {
        super(message);
        this.exitCode = exitCode;
    }

    /**
     * Returns the exit code associated with this exception.
     *
     * @return the exit code
     */
    public int exitCode() {
        return exitCode;
    }

    /**
     * Exits the application with the specified exit code and message.
     * <p>checkRunningFromCommandLine() determined this program is running from the command line.
     * <p>In the future, this should be moved to Chronicle-Core.
     *
     * @param exitCode the exit code to use when terminating the application
     * @param message  the message to display upon exit
     * @return
     */
    @SuppressWarnings("java:S106") // System.err is acceptable here
    public static ExitCodeRuntimeException orExit(int exitCode, @Nullable String message) {
        boolean runningFromCommandLine = examineTheStackToDetermineIfItShouldExit();
        if (runningFromCommandLine) {
            System.err.println(message == null ? "Exiting with code: " + exitCode : message);
            System.exit(exitCode);

        }
        return new ExitCodeRuntimeException(exitCode, message == null ? "Exiting with code: " + exitCode : message);
    }

    private static boolean examineTheStackToDetermineIfItShouldExit() {
        // if we are not the "main" thread, then we are not running from the command line
        // this effectively rules out non main threads triggering a System.exit()
        if (!Thread.currentThread().getName().equals("main")) {
            return false;
        }
        // if we are the main thread, look for more than one main method on the stack
        // if more than one main method is found, we are not running from the command line, rather nested, possibly in junit.
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        int mainMethodCount = 0;
        for (StackTraceElement element : stackTrace) {
            if (element.getMethodName().equals("main")) {
                mainMethodCount++;
            }
        }
        return mainMethodCount == 1;
    }
}
