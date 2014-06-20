package demo;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by daniel on 19/06/2014.
 */
public class ChronicleDashboard implements ChronicleUpdatable{
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
    private AtomicLong messagesProduced = new AtomicLong(0);
    private AtomicLong messagesRead = new AtomicLong(0);
    private AtomicLong runningTime = new AtomicLong(0);
    private AtomicLong tcpMessagesProduced = new AtomicLong(0);
    private File file = new File("/");
    private ChronicleController controller;

    public ChronicleDashboard() {
    }

    public void init(){
        controller = new ChronicleController(this);
        final GUIUpdaterThread updater = new GUIUpdaterThread();
        updater.start();

        startButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if(startButton.getText().equals("Start")){
                    startButton.setText("Stop");
                    resetButton.setEnabled(false);
                    updater.go();
                    controller.start((String)cbRate.getSelectedItem());
                }else{
                    startButton.setText("Start");
                    resetButton.setEnabled(true);
                    controller.stop();
                    updater.pause();
                }
            }
        });

        resetButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                reset();
            }
        });


        cbRate.addItem("1000");
        cbRate.addItem("5000");
        cbRate.addItem("25000");
        cbRate.addItem("125000");
        cbRate.addItem("625000");
        cbRate.addItem("MAX");

        cbRate.setSelectedItem("MAX");

        reset();
    }

    public void reset(){
        messagesProduced.set(0);
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
        controller.reset();
        tfDiskSpace.setText(getBytesAsGB(file.getUsableSpace()));
    }

    public void messageProduced(){
        messagesProduced.addAndGet(1);
    }

    @Override
    public void setFileNames(List<String> files) {
        String fileNames = "";
        for(int i=0; i<files.size(); i++){
            if(i!=0)fileNames+="\n";
            fileNames+=files.get(i);
        }
        tpFiles.setText(fileNames);
        System.out.println("Setting files " + fileNames);
    }

    @Override
    public void addTimeMillis(long l) {
        runningTime.addAndGet(l);
    }

    @Override
    public void messageRead() {
        messagesRead.addAndGet(1);
    }

    @Override
    public void tcpMessageRead() {
        tcpMessagesProduced.addAndGet(1);
    }

    public String getBytesAsGB(long bytes)
    {
        double step = Math.pow(1000, 3);
        if (bytes > step) return String.format("%3.1f %s", bytes / step, "GB");
        return Long.toString(bytes);
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("ChronicleDashboard");
        ChronicleDashboard chronicleDashboard = new ChronicleDashboard();
        frame.setContentPane(chronicleDashboard.mainPanel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setSize(800,500);
        frame.setVisible(true);

        chronicleDashboard.init();
    }

    private void createUIComponents() {
        // TODO: place custom component creation code here
    }

    private class GUIUpdaterThread extends Thread{
        private AtomicBoolean isRunning = new AtomicBoolean(false);

        private long count=0;
        public void run(){
            while(true){
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if(isRunning.get()) {
                    tfTCPMessages.setText(String.valueOf(tcpMessagesProduced));
                    tfMessagesRead.setText(String.valueOf(messagesRead));
                    tfMessagesWritten.setText(String.valueOf(messagesProduced));
                    tfRunningTime.setText(String.valueOf(runningTime));
                    if(runningTime.get() !=0)
                    tfActualReadRate.setText(String.valueOf((int)(messagesRead.get()/(runningTime.get()/1000.0))));
                    tfTCPReads.setText(String.valueOf((int)(tcpMessagesProduced.get()/(runningTime.get()/1000.0))));
                    tfActualWriteRate.setText(String.valueOf((int)(messagesProduced.get()/(runningTime.get()/1000.0))));
                    if(count % 5 == 0){
                        //Once a second read file space
                        tfDiskSpace.setText(getBytesAsGB(file.getUsableSpace()));
                    }
                    count++;
                }

            }
        }

        public void pause(){
            isRunning.set(false);
        }

        public void go(){
            isRunning.set(true);
        }
    }
}
