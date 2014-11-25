package demo;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by daniel on 24/11/2014.
 */
public class ChronicleDashboard2 implements ChronicleUpdatable{

    private AtomicLong runningTime = new AtomicLong(0);
    private AtomicLong messagesRead = new AtomicLong(0);
    private AtomicLong tcpMessagesProduced = new AtomicLong(0);
    private AtomicLong messagesProduced1 = new AtomicLong(0);
    private AtomicLong messagesProduced2 = new AtomicLong(0);
    private JTextField tfTotalWrites = new JTextField();
    private JTextField tfWriteRate = new JTextField();
    private JTextField tfTotalReads = new JTextField();
    private JTextField tfReadRate = new JTextField();
    private JTextField tfTotalTCP = new JTextField();
    private JTextField tfRunningTime = new JTextField();
    private JTextField tfTCPRate = new JTextField();
    private JTextPane tpFiles = new JTextPane();
    private JTextField tfDiskSpace = new JTextField();
    private File demo_path = new File((System.getProperty("java.io.tmpdir") + "/demo").replaceAll("//", "/"));

    private ChronicleController controller;
    public static void main(String... args) throws IOException{
        new ChronicleDashboard2();
    }
    public ChronicleDashboard2() throws IOException{
        Image image = ImageIO.read(ChronicleDashboard2.class.getResourceAsStream("/diagram.jpg"));

        final GUIUpdaterThread updater = new GUIUpdaterThread();
        updater.setLoopTime(100);
        updater.start();
        controller = new ChronicleController(this, demo_path);

        JFrame frame = new JFrame("ChronicleDashboard");
        BackgroundPanel bg = new BackgroundPanel(image);

        JTextArea info = new JTextArea();
        info.setText("This demonstrates how ChronicleQueue " +
                     "might perform on your machine.\n" +
                     "Messages (prices consisting of 1 String, " +
                     "4 ints, 1 bool) flow round the system " +
                     "topology described by the diagram.\n" +
                     "An average laptop should be able to " +
                     "process 1.5m messages/second.");

        final JButton startButton = new JButton("Start Demo");
        JLabel lblRate = new JLabel("Select event rate/s:");
        final JComboBox<String> cbRate = new JComboBox<>();
        cbRate.addItem("     10,000");
        cbRate.addItem("     30,000");
        cbRate.addItem("   100,000");
        cbRate.addItem("   300,000");
        cbRate.addItem("1,000,000");
        cbRate.addItem("3,000,000");
        cbRate.addItem("MAX");

        cbRate.setSelectedItem("1,000,000");

        final JProgressBar pBar = new JProgressBar();
        JLabel lblEventsWritten = new JLabel("Events written");
        JLabel lblRateWritten = new JLabel("Write rate(p/s)");
        JLabel lblEventsRead = new JLabel("Events read");
        JLabel lblRateRead = new JLabel("Read rate(p/s)");
        JLabel lblEventsTCP = new JLabel("Events read");
        JLabel lblRateTCP = new JLabel("Read rate(p/s)");
        JLabel lblRunningTime = new JLabel("Demo running time(s)");
        JLabel lblFilesCreated = new JLabel("Files written to disk");
        JLabel lblDiskSpace = new JLabel("Disk space remaining");

        JScrollPane scrollPane = new JScrollPane(tpFiles);
        scrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);

        bg.add(tfTotalWrites);
        bg.add(tfWriteRate);
        bg.add(startButton);
        bg.add(lblRate);
        bg.add(cbRate);
        bg.add(pBar);
        bg.add(lblEventsWritten);
        bg.add(lblRateWritten);
        bg.add(tfReadRate);
        bg.add(tfTotalReads);
        bg.add(lblEventsRead);
        bg.add(lblRateRead);
        bg.add(tfTCPRate);
        bg.add(tfTotalTCP);
        bg.add(lblEventsTCP);
        bg.add(lblRateTCP);
        bg.add(tfRunningTime);
        bg.add(lblRunningTime);
        bg.add(scrollPane);
        bg.add(lblFilesCreated);
        bg.add(lblDiskSpace);
        bg.add(tfDiskSpace);
        bg.add(info)
;
        startButton.setLocation(5, 5);
        startButton.setSize(100, 50);

        lblRate.setLocation(120, 5);
        lblRate.setSize(140, 18);
        cbRate.setLocation(120, 25);
        cbRate.setSize(140, 25);

        lblRateWritten.setLocation(90, 180);
        lblRateWritten.setSize(100, 18);
        tfWriteRate.setLocation(200, 180);
        tfWriteRate.setSize(80, 18);
        tfWriteRate.setEditable(false);

        lblEventsWritten.setLocation(90, 200);
        lblEventsWritten.setSize(100, 18);
        tfTotalWrites.setLocation(200, 200);
        tfTotalWrites.setSize(80, 18);
        tfTotalWrites.setEditable(false);

        lblRateRead.setLocation(160, 320);
        lblRateRead.setSize(100, 18);
        tfReadRate.setLocation(270, 320);
        tfReadRate.setSize(80, 18);
        tfReadRate.setEditable(false);

        lblEventsRead.setLocation(160, 340);
        lblEventsRead.setSize(100, 18);
        tfTotalReads.setLocation(270, 340);
        tfTotalReads.setSize(80, 18);
        tfTotalReads.setEditable(false);

        lblRateTCP.setLocation(610, 100);
        lblRateTCP.setSize(100, 18);
        tfTCPRate.setLocation(720, 100);
        tfTCPRate.setSize(80, 18);
        tfTCPRate.setEditable(false);

        lblEventsTCP.setLocation(610, 120);
        lblEventsTCP.setSize(100, 18);
        tfTotalTCP.setLocation(720, 120);
        tfTotalTCP.setSize(80, 18);
        tfTotalTCP.setEditable(false);

        lblRunningTime.setLocation(5, 75);
        lblRunningTime.setSize(150, 18);
        tfRunningTime.setLocation(160, 75);
        tfRunningTime.setSize(100, 18);
        tfRunningTime.setEditable(false);

        int dx = 510;
        int dy = 350;

        lblFilesCreated.setLocation(dx, dy);
        lblFilesCreated.setSize(200, 18);

        scrollPane.setLocation(dx, dy+20);
        scrollPane.setSize(320, 150);
        scrollPane.setEnabled(false);

        lblDiskSpace.setLocation(dx, dy+175);
        lblDiskSpace.setSize(140, 18);

        tfDiskSpace.setLocation(dx+140, dy+175);
        tfDiskSpace.setSize(100, 18);
        tfDiskSpace.setEditable(false);

        info.setLocation(60, 380);
        info.setSize(215, 145);
        info.setEditable(false);
        info.setOpaque(false);
        info.setFont(info.getFont().deriveFont(12.0f));
        info.setLineWrap(true);
        info.setWrapStyleWord(true);

        pBar.setLocation(5, 60);
        pBar.setSize(250, 10);
        pBar.setIndeterminate(false);

        startButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if(startButton.getText().equals("Start Demo")){
                    startButton.setText("Stop Demo");
                    pBar.setIndeterminate(true);
                    cbRate.setEnabled(false);
                    updater.go();
                    try {
                        controller.start((String) cbRate.getSelectedItem());
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }else if(startButton.getText().equals("Stop Demo")){
                    startButton.setText("Reset Demo");
                    pBar.setIndeterminate(false);
                    updater.pause();
                    controller.stop();
                }else if(startButton.getText().equals("Reset Demo")){
                    startButton.setText("Start Demo");
                    cbRate.setEnabled(true);
                    messagesProduced1.set(0);
                    messagesProduced2.set(0);
                    tcpMessagesProduced.set(0);
                    messagesRead.set(0);
                    runningTime.set(0);
                    tfTotalWrites.setText("0");
                    tfRunningTime.setText("0");
                    tfWriteRate.setText("0");
                    tfReadRate.setText("0");
                    tfTotalTCP.setText("0");
                    tfTotalReads.setText("0");
                    tfTCPRate.setText("0");
                    tfDiskSpace.setText(getBytesAsGB(demo_path.getUsableSpace()));
                    tpFiles.setText("");
                }
            }
        });

        frame.setContentPane(bg);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().repaint();
        frame.pack();
        frame.setResizable(false);
        frame.setVisible(true);
    }

    public String getBytesAsGB(long bytes) {
        double step = Math.pow(1000, 3);
        if (bytes > step) return String.format("%3.1f %s", bytes / step, "GB");
        return Long.toString(bytes);
    }

    @Override
    public void setFileNames(List<String> files) {
        String fileNames = "";
        for (int i = 0; i < files.size(); i++) {
            if (i != 0) fileNames += "\n";
            fileNames += files.get(i);
        }
        tpFiles.setText(fileNames);
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
    @Override
    public AtomicLong count1() {
        return messagesProduced1;
    }

    @Override
    public AtomicLong count2() {
        return messagesProduced2;
    }
    private static class BackgroundPanel extends JPanel
    {
        private Image image;
        public BackgroundPanel(Image image)
        {
            try
            {
                this.image = image;

                Dimension size = new Dimension(image.getWidth(null), image.getHeight(null));
                setPreferredSize(size);
                setMinimumSize(size);
                setMaximumSize(size);
                setSize(size);
                setLayout(null);
            }
            catch (Exception e) { e.printStackTrace();/*handled in paintComponent()*/ }
        }

        @Override
        protected void paintComponent(Graphics g)
        {
            super.paintComponent(g);
            if (image != null)
                g.drawImage(image, 0,0,this.getWidth(),this.getHeight(),this);
        }
    }

    private class GUIUpdaterThread extends ControlledThread {
        private long count = 0;

        @Override
        public void loop() {
            tfTotalTCP.setText(String.format("%,d K", tcpMessagesProduced.get() / 1000));
            tfTotalReads.setText(String.format("%,d K", messagesRead.get() / 1000));
            long totalMessage = messagesProduced1.get() + messagesProduced2.get();
            tfTotalWrites.setText(String.format("%,d K", totalMessage / 1000));
            long runningTime = ChronicleDashboard2.this.runningTime.get();
            tfRunningTime.setText(String.format("%.3f", runningTime / 1000.0));
            if (runningTime != 0) {
                tfReadRate.setText(String.format("%,d K", messagesRead.get() / runningTime));
                tfTCPRate.setText(String.format("%,d K", tcpMessagesProduced.get() / runningTime));
                tfWriteRate.setText(String.format("%,d K", totalMessage / runningTime));
            }
            if (count % 5 == 0) {
                //Once a second read file space
                tfDiskSpace.setText(getBytesAsGB(demo_path.getUsableSpace()));
            }
            count++;
        }

        @Override
        public void cleanup() {
        }
    }
}
