package net.openhft.chronicle.queue.common;

import net.openhft.chronicle.core.Jvm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProcessRunner {

    /**
     * Spawn a process running the main method of a specified class
     *
     * @param clazz The class to execute
     * @param args  Any arguments to pass to the process
     * @return the Process spawned
     * @throws IOException if there is an error starting the process
     */
    public static Process runClass(Class<?> clazz, String... args) throws IOException {
        String classPath = System.getProperty("java.class.path");
        String className = clazz.getName();
        String javaBin = findJavaBinPath().toString();
        List<String> allArgs = new ArrayList<>();
        allArgs.add(javaBin);
        allArgs.add("-cp");
        allArgs.add(classPath);
        allArgs.add(className);
        allArgs.addAll(Arrays.asList(args));
        ProcessBuilder processBuilder = new ProcessBuilder(allArgs.toArray(new String[]{}));
        return processBuilder.start();
    }

    /**
     * Log stdout and stderr for a process
     * <p>
     * ProcessBuilder.inheritIO() didn't play nicely with Maven failsafe plugin
     * <p>
     * https://maven.apache.org/surefire/maven-failsafe-plugin/faq.html#corruptedstream
     */
    public static void printProcessOutput(String processName, Process process) {
        Jvm.startup().on(ProcessRunner.class, "\n"
                + "Output for " + processName + "\n"
                + "stdout:\n"
                + copyStreamToString(process.getInputStream()) + "\n"
                + "stderr:\n"
                + copyStreamToString(process.getErrorStream()));
    }

    /**
     * Copies a stream to a string, up to the point where reading more would block
     *
     * @param inputStream The stream to read from
     * @return The output as a string
     */
    private static String copyStreamToString(InputStream inputStream) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        try {
            while (inputStream.available() > 0 && (read = inputStream.read(buffer)) >= 0) {
                os.write(buffer, 0, read);
            }
        } catch (IOException e) {
            // Ignore
        }
        return new String(os.toByteArray(), Charset.defaultCharset());
    }

    /**
     * Try and work out what the java executable is cross platform
     *
     * @return the Path to the java executable
     * @throws IllegalStateException if the executable couldn't be located
     */
    private static Path findJavaBinPath() {
        final Path javaBinPath = Paths.get(System.getProperty("java.home")).resolve("bin");
        final Path linuxJavaExecutable = javaBinPath.resolve("java");
        if (linuxJavaExecutable.toFile().exists()) {
            return linuxJavaExecutable;
        } else {
            Path windowsJavaExecutable = javaBinPath.resolve("java.exe");
            if (windowsJavaExecutable.toFile().exists()) {
                return windowsJavaExecutable;
            }
        }
        throw new IllegalStateException("Couldn't locate java executable!");
    }
}
