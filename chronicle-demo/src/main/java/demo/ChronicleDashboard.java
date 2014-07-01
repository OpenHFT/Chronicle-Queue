package demo;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by daniel on 19/06/2014.
 */
public class ChronicleDashboard implements ChronicleUpdatable {
    private JPanel mainPanel;
    private JButton startButton;
    private JTextField tfMessagesRead;
    private JComboBox cbRate;
    private JTextField tfTCPMessages;
    private JTextField tfActualReadRate;
    private JButton resetButton;
    private JTextField tfMessagesWritten;
    private JTextPane tpFiles;
    private JTextField tfActualWriteRate;
    private JTextField tfRunningTime;
    private JTextField tfDiskSpace;
    private JTextField tfTCPReads;
    private AtomicLong messagesProduced1 = new AtomicLong(0);
    private AtomicLong messagesProduced2 = new AtomicLong(0);
    private AtomicLong messagesRead = new AtomicLong(0);
    private AtomicLong runningTime = new AtomicLong(0);
    private AtomicLong tcpMessagesProduced = new AtomicLong(0);
    private File demo_path = new File((System.getProperty("java.io.tmpdir") + "/demo").replaceAll("//", "/"));

    private ChronicleController controller;

    public ChronicleDashboard() {
    }

    public void init() throws IOException {
        final GUIUpdaterThread updater = new GUIUpdaterThread();
        updater.start();

        resetButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                reset();
            }
        });


        cbRate.addItem("     10,000");
        cbRate.addItem("     30,000");
        cbRate.addItem("   100,000");
        cbRate.addItem("   300,000");
        cbRate.addItem("1,000,000");
        cbRate.addItem("3,000,000");
        cbRate.addItem("MAX");

        cbRate.setSelectedItem("1,000,000");

        reset();

        controller = new ChronicleController(this, demo_path);

        startButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (startButton.getText().equals("Start")) {
                    startButton.setText("Stop");
                    resetButton.setEnabled(false);
                    updater.go();
                    controller.start((String) cbRate.getSelectedItem());
                } else {
                    startButton.setText("Start");
                    resetButton.setEnabled(true);
                    controller.stop();
                    updater.pause();
                }
            }
        });
    }

    public void reset() {
        messagesProduced1.set(0);
        messagesProduced2.set(0);
        tcpMessagesProduced.set(0);
        messagesRead.set(0);
        runningTime.set(0);
        tfMessagesWritten.setText("0");
        tfRunningTime.setText("0");
        tfActualWriteRate.setText("0");
        tfActualReadRate.setText("0");
        tfTCPMessages.setText("0");
        tfMessagesRead.setText("0");
        tfTCPReads.setText("0");
        tfDiskSpace.setText(getBytesAsGB(demo_path.getUsableSpace()));
        if (controller != null)
            controller.reset();
    }


    @Override
    public void setFileNames(List<String> files) {
        String fileNames = "";
        for (int i = 0; i < files.size(); i++) {
            if (i != 0) fileNames += "\n";
            fileNames += files.get(i);
        }
        tpFiles.setText(fileNames);
//        System.out.println("Setting files " + fileNames);
    }

    @Override
    public void addTimeMillis(long l) {
        runningTime.addAndGet(l);
    }

    @Override
    public void incrMessageRead() {
        messagesRead.incrementAndGet();
    }

    @Override
    public void incrTcpMessageRead() {
        tcpMessagesProduced.incrementAndGet();
    }

    @Override
    public AtomicLong tcpMessageRead() {
        return tcpMessagesProduced;
    }

    public String getBytesAsGB(long bytes) {
        double step = Math.pow(1000, 3);
        if (bytes > step) return String.format("%3.1f %s", bytes / step, "GB");
        return Long.toString(bytes);
    }

    public static void main(String[] args) {
        try {
            JFrame frame = new JFrame("ChronicleDashboard");
            ChronicleDashboard chronicleDashboard = new ChronicleDashboard();
            frame.setContentPane(chronicleDashboard.mainPanel);
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.pack();
            frame.setSize(800, 500);
            frame.setVisible(true);

            chronicleDashboard.init();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    private void createUIComponents() {
        // TODO: place custom component creation code here
    }

    @Override
    public AtomicLong count1() {
        return messagesProduced1;
    }

    @Override
    public AtomicLong count2() {
        return messagesProduced2;
    }

    private class GUIUpdaterThread extends Thread {
        private AtomicBoolean isRunning = new AtomicBoolean(false);

        private long count = 0;

        public void run() {
            while (true) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (isRunning.get()) {
                    tfTCPMessages.setText(String.format("%,d K", tcpMessagesProduced.get() / 1000));
                    tfMessagesRead.setText(String.format("%,d K", messagesRead.get() / 1000));
                    long totalMessage = messagesProduced1.get() + messagesProduced2.get();
                    tfMessagesWritten.setText(String.format("%,d K", totalMessage / 1000));
                    long runningTime = ChronicleDashboard.this.runningTime.get();
                    tfRunningTime.setText(String.format("%.3f sec", runningTime / 1000.0));
                    if (runningTime != 0) {
                        tfActualReadRate.setText(String.format("%,d K", messagesRead.get() / runningTime));
                        tfTCPReads.setText(String.format("%,d K", tcpMessagesProduced.get() / runningTime));
                        tfActualWriteRate.setText(String.format("%,d K", totalMessage / runningTime));
                    }
                    if (count % 5 == 0) {
                        //Once a second read file space
                        tfDiskSpace.setText(getBytesAsGB(demo_path.getUsableSpace()));
                    }
                    count++;
                }

            }
        }

        public void pause() {
            isRunning.set(false);
        }

        public void go() {
            isRunning.set(true);
        }
    }
}
