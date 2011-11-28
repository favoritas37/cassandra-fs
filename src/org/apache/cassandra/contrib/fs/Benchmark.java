package org.apache.cassandra.contrib.fs;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.String;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.cassandra.contrib.fs.cli.FSCliMain;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.transport.TTransportException;

public class Benchmark {

    private FSCliMain cli;
    //private PrintStream out = System.out;
    private HashSet<String> filesCreated;
    private HashSet<String> foldersCreated;
    private final String baseUsrFolder = "/usr";
    private static Logger LOGGER = Logger.getLogger(Benchmark.class);
    private HashMap<String, ArrayList<OpInfo>> results = new HashMap<String, ArrayList<OpInfo>>();
    private HashMap<String, String> fileToFolder = new HashMap<String, String>();
    private ArrayList<String> filesUsed = new ArrayList<String>();
    private BufferedWriter out;
    int smallFileSize = 0;

    private String synchServerIP;
    private int synchServerPort;
    private int numOfSmallFiles;
    private int numOfLargeFiles;
    private boolean writeOnly;

    public class OpInfo {

        public long duration = 0;
        public int fileLength = 0;
        public String operation = "none";
    }

    public Benchmark() throws IOException {
        try {
            cli = new FSCliMain();
        } catch (IOException ex) {
            LOGGER.error(ex);
        } catch (TTransportException ex) {
            LOGGER.error(ex);
        }

        filesCreated = new HashSet<String>();
        foldersCreated = new HashSet<String>();
        results = new HashMap<String, ArrayList<OpInfo>>();
        results.put("newFile", new ArrayList<OpInfo>());
        results.put("rmr", new ArrayList<OpInfo>());
        results.put("rm", new ArrayList<OpInfo>());
        results.put("copyToLocal", new ArrayList<OpInfo>());
        results.put("copyFromLocal", new ArrayList<OpInfo>());
        results.put("ls", new ArrayList<OpInfo>());
        results.put("mkdir", new ArrayList<OpInfo>());

        File clientConfFile = new File("client-conf.properties");
        if (!clientConfFile.exists()) {
            throw new RuntimeException("'" + clientConfFile.getAbsolutePath()
                    + "' does not exist!");
        }

        ClientConfiguration conf = new ClientConfiguration(clientConfFile.getAbsolutePath());
        String[] serverInfo = conf.getBenchmarkSynchServerIp().split(":");
        synchServerIP = serverInfo[0];
        synchServerPort = Integer.parseInt(serverInfo[1]);
        numOfSmallFiles = conf.getBenchmarkNumOfSmallFiles();
        numOfLargeFiles = conf.getBenchmarkNumOfLargeFiles();
        writeOnly = conf.isWriteOnly();
    }

    public void start() throws IOException, TTransportException {
       cli.connect();

        System.out.println("Connecting to server: " + synchServerIP + ":" + synchServerPort);
        Socket clientSocket = new Socket(synchServerIP, synchServerPort);
        
        PrintWriter outToServer = new PrintWriter(clientSocket.getOutputStream(), true);
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
         
        char[] cbuf = new char[5];

        String[] commandsStr = new String[6]; 
        commandsStr[0] = "metad";
        commandsStr[1] = "mkdir";
        commandsStr[2] = "cpfl_";
        commandsStr[3] = "cptl_";
        commandsStr[4] = "rmr__";
        commandsStr[5] = "exit_";
        
        int i=0;
        while(true)
        {
            LOGGER.debug("Receiving command from server");
            inFromServer.read(cbuf);
            LOGGER.debug("Receiving command succeded");
            //String command = commandsStr[i++];//new String(cbuf);
            String command = new String(cbuf);

            if(command.compareTo("exit_") == 0)
                break;
            else if(command.compareTo("metad") == 0)
            {
               createFiles(numOfSmallFiles, 10, 10240, 7);
            }
            else if(command.compareTo("mkdir") == 0)
            {
                initializeFiletranferBencmark(7);
            }
            else if(command.compareTo("cpfl_") == 0)
            {
                copyFromLocal(numOfLargeFiles);
            }
            else if(command.compareTo("cptl_") == 0)
            {
                if(!writeOnly)
                    copyToLocal(numOfLargeFiles);
            }
            else if(command.compareTo("rmr__") == 0)
            {
                deleteCreatedFiles();
            }

            LOGGER.debug("Sending ACK: "  + command);
            outToServer.println(command);
            LOGGER.debug("Sending succeded");
        }

        outToServer.close();
        inFromServer.close();
    }

    /**
     *
     * @param maxSizeOfFile -> num of bytes
     * @param maxFolderDepth
     */
    public void createFiles(int numberOfFiles, int minSizeOfFile, int maxSizeOfFile, int maxFolderDepth) {

        for (int i = 0; i < numberOfFiles; i++) {
            createSingleFile(minSizeOfFile, maxSizeOfFile, maxFolderDepth);
        }
    }

    public void createSingleFile(int minSizeOfFile, int maxSizeOfFile, int maxFolderDepth) {
        int depth = RandomGenerator.rand(2, maxFolderDepth);
        String path = baseUsrFolder;

        //generate the folder names
        for (int i = 1; i < depth; i++) {
            path += "/" + RandomGenerator.randomstring(3, 7);
            if(i==1)
                foldersCreated.add(path);
        }

        //create file name
        path += "/" + RandomGenerator.randomstring(3, 7);

        //generateContent
        int contentLength = RandomGenerator.rand(minSizeOfFile, maxSizeOfFile);
        char[] content = new char[contentLength];
        char element = '1';
        Arrays.fill(content, element);

        String command = "newFile " + path + " " + new String(content);

        long startTime = System.currentTimeMillis();
        cli.processCommand(command);
        long endTime = System.currentTimeMillis();

        OpInfo oInfo = new OpInfo();
        oInfo.duration = endTime - startTime;
        oInfo.fileLength = content.length;
        oInfo.operation = "newFile";
        results.get("newFile").add(oInfo);
        //results

        LOGGER.debug("File created: " + path + ", in " + oInfo.duration + "ms");
        filesCreated.add(path);
    }

    public void deleteCreatedFiles() {
        Iterator<String> it = foldersCreated.iterator();

        while (it.hasNext()) {
            String folder = it.next();

            LOGGER.debug("-------------------------Deleting folder: " + folder + "-----------------------------");

            long startTime = System.currentTimeMillis();
            cli.processCommand("rmr " + folder);
            long endTime = System.currentTimeMillis();

            OpInfo oInfo = new OpInfo();
            oInfo.duration = endTime - startTime;
            oInfo.fileLength = 0;
            oInfo.operation = "rmr";
            results.get("rmr").add(oInfo);
        }
    }

    public void initializeFiletranferBencmark(int maxFolderDepth) {
        //set the path where the 24 files will be put.
        for (int i = 0; i < 24; i++) {
            int depth = RandomGenerator.rand(2, maxFolderDepth);
            //generate the folder names
            String path = baseUsrFolder;
            for (int j = 1; j < depth; j++) {
                path += "/" + RandomGenerator.randomstring(3, 7);
                if(j==1)
                    foldersCreated.add(path);
            }

            long startTime = System.currentTimeMillis();
            cli.processCommand("mkdir " + path);
            long endTime = System.currentTimeMillis();

            OpInfo oInfo = new OpInfo();
            oInfo.duration = endTime - startTime;
            oInfo.operation = "mkdir";
            results.get("mkdir").add(oInfo);

            char file = ((char) ((int) 'a' + i));
            fileToFolder.put("" + file, path);
        }
    }

    public void copyFromLocal(int numOfOperations) {
        String filePositionOnDisk = "C:\\tmpFiles\\";

        File smallfile = new File(filePositionOnDisk + "a");

        if (smallfile.exists() && smallfile.isFile()) {
            smallFileSize = (int) smallfile.length();
        }

        for (int i = 0; i < numOfOperations; i++) {
            int index = RandomGenerator.rand(0, 23);

            char file = ((char) ((int) 'a' + index));
            String filePath = fileToFolder.get("" + file) + "/"  + file;
            filesUsed.add(filePath);
            LOGGER.debug("Remote path selected to place file: " + filePath);

            File curfile = new File(filePositionOnDisk + file);

            //execute
            long startTime = System.currentTimeMillis();
            cli.processCommand("copyFromLocal " + filePositionOnDisk + file + " " + filePath);
            long endTime = System.currentTimeMillis();

            OpInfo oInfo = new OpInfo();
            oInfo.duration = endTime - startTime;
            oInfo.fileLength = (int)(curfile.length());
            oInfo.operation = "copyFromLocal";
            results.get("copyFromLocal").add(oInfo);
        }
    }

    public void copyToLocal(int numOfOperations) {
        String outputFolder = "C:\\outputFolder\\";

        for (int i = 0; i < numOfOperations; i++)
        {
            String curFile = filesUsed.get(i);
            char file = curFile.charAt(curFile.length()-1);

            long startTime = System.currentTimeMillis();
            cli.processCommand("copyToLocal " + curFile + " " + outputFolder + file);
            long endTime = System.currentTimeMillis();

            File curfile = new File(outputFolder + file);

            OpInfo oInfo = new OpInfo();
            oInfo.duration = endTime - startTime;
            oInfo.fileLength = (int)curfile.length();
            oInfo.operation = "copyToLocal";
            results.get("copyToLocal").add(oInfo);
        }
    }

    public void createStatistics() throws IOException {
        out = new BufferedWriter(new FileWriter("report" + ManagementFactory.getRuntimeMXBean().getName() + ".txt"));
        newFileStatistics();
        mkdirStatistics();
        copyFromLocalStatistics();
        copyToLocalStatistics();
        rmrStatistics();
        out.close();
    }

    public void newFileStatistics() throws IOException {
        ArrayList<OpInfo> oInfoList = results.get("newFile");
        long totalFileLength = 0;
        long totalDuration = 0;

        for (int i = 0; i < oInfoList.size(); i++) {
            OpInfo inf = oInfoList.get(i);
            totalFileLength += inf.fileLength;
            totalDuration += inf.duration;
        }

        out.write("----------Creating new files------------\n");
        out.write(String.format("Total duration: %.2f sec\n", (double) totalDuration / 1000));
        out.write(String.format("Total bytes created: %.2f KB\n", (double) totalFileLength / 1024));
        out.write(String.format("Average throughput: %.2f KB/s\n", (((double) totalFileLength * 1000) / (double) totalDuration) / 1024));
        out.write(String.format("Average operations per second: %.2f op/s\n", ((double) oInfoList.size() * 1000) / (double) totalDuration));
        out.write(String.format("Average time per operation: %.2f ms\n", (double) totalDuration / (double) oInfoList.size()));
    }

    public void rmrStatistics() throws IOException {
        ArrayList<OpInfo> oInfoList = results.get("rmr");
        long totalDuration = 0;

        for (int i = 0; i < oInfoList.size(); i++) {
            OpInfo inf = oInfoList.get(i);
            totalDuration += inf.duration;
        }

        out.write("----------Deleting all folders created statistics------------\n");
        out.write(String.format("Total duration: %.2f sec\n", (double) totalDuration / 1000));
        out.write(String.format("Average operations per second: %.2f op/s\n", ((double) oInfoList.size() * 1000) / (double) totalDuration));
        out.write(String.format("Average time per operation: %.2f ms\n", (double) totalDuration / (double) oInfoList.size()));
    }

    public void copyToLocalStatistics() throws IOException {
        ArrayList<OpInfo> oInfoList = results.get("copyToLocal");
        long totalFileLength = 0;
        long totalDuration = 0;

        for (int i = 0; i < oInfoList.size(); i++) {
            OpInfo inf = oInfoList.get(i);
            totalFileLength += inf.fileLength;
            totalDuration += inf.duration;
        }

        out.write("----------Copying files from FS to local------------\n");
        out.write(String.format("Total duration: %.2f sec\n", (double) totalDuration / 1000));
        out.write(String.format("Total bytes created: %.2f KB\n", (double) totalFileLength / 1024));
        out.write(String.format("Average throughput: %.2f KB/s\n", (((double) totalFileLength * 1000) / (double) totalDuration) / 1024));
        out.write(String.format("Average operations per second: %.2f op/s\n", ((double) oInfoList.size() * 1000) / (double) totalDuration));
        out.write(String.format("Average time per operation: %.2f ms\n", (double) totalDuration / (double) oInfoList.size()));
    }

    public void copyFromLocalStatistics() throws IOException {
        ArrayList<OpInfo> oInfoList = results.get("copyFromLocal");
        long totalFileLength = 0;
        long totalDuration = 0;

        for (int i = 0; i < oInfoList.size(); i++) {
            OpInfo inf = oInfoList.get(i);
            totalFileLength += inf.fileLength;
            totalDuration += inf.duration;
        }

        out.write("----------Uploading files to FS------------\n");
        out.write(String.format("Total duration: %.2f sec\n", (double) totalDuration / 1000));
        out.write(String.format("Total bytes created: %.2f KB\n", (double) totalFileLength / 1024));
        out.write(String.format("Average throughput: %.2f KB/s\n", (((double) totalFileLength * 1000) / (double) totalDuration) / 1024));
        out.write(String.format("Average operations per second: %.2f op/s\n", ((double) oInfoList.size() * 1000) / (double) totalDuration));
        out.write(String.format("Average time per operation: %.2f ms\n", (double) totalDuration / (double) oInfoList.size()));
    }

    public void mkdirStatistics() throws IOException {
        ArrayList<OpInfo> oInfoList = results.get("mkdir");
        long totalDuration = 0;

        for (int i = 0; i < oInfoList.size(); i++) {
            OpInfo inf = oInfoList.get(i);
            totalDuration += inf.duration;
        }

        out.write("----------Making Directories to FS------------\n");
        out.write(String.format("Total duration: %.2f sec\n", (double) totalDuration / 1000));
        out.write(String.format("Average operations per second: %.2f op/s\n", ((double) oInfoList.size() * 1000) / (double) totalDuration));
        out.write(String.format("Average time per operation: %.2f ms\n", (double) totalDuration / (double) oInfoList.size()));
    }

    public static void main(String[] args) throws IOException, TTransportException {
        PropertyConfigurator.configure("log4j.properties");

        //System.in.read();

        Benchmark bm = new Benchmark();
        bm.start();

        bm.createStatistics();

        System.exit(0);
    }
}
