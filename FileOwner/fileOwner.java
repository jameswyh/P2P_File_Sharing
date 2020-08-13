import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class fileOwner {
    public static int port;
    public static void main(String[] args) throws Exception{
    	port = Integer.parseInt(args[0]);
        File file = new File(args[1]);
        FileInputStream inputStream = new FileInputStream(file);
        System.out.println(file.length());
        int numberOfChunks = (int) (file.length() / (100 * 1024));
        if (file.length() % 100 != 0)
            numberOfChunks++;
        File[] chunks = new File[numberOfChunks];
        for (int i = 0; i < numberOfChunks; i++) {
            chunks[i] = new File("chunk" + i);
            FileOutputStream outputStream = new FileOutputStream(chunks[i]);
            for (int j = 1; j <= 100; j++) {
                byte[] b = new byte[1024];
                int len = inputStream.read(b);
                if (len == -1)
                    break;
                byte[] bb = new byte[len];
                for (int index = 0; index < len; index++)
                    bb[index] = b[index];
 
                outputStream.write(bb);
            }
            System.out.println("Cur Length: " + chunks[i].length());
            outputStream.close();
        }
        inputStream.close();
        ServerSocket listener = new ServerSocket(port);
        List<File>[] lists = new List[5];
        List<File> list1 = new ArrayList<>();
        List<File> list2 = new ArrayList<>();
        List<File> list3 = new ArrayList<>();
        List<File> list4 = new ArrayList<>();
        List<File> list5 = new ArrayList<>();

        for (int i = 0; i < numberOfChunks; i++) {
            list1.add(chunks[i++]);
            if (i == numberOfChunks)
                break;
            list2.add(chunks[i++]);
            if (i == numberOfChunks)
                break;
            list3.add(chunks[i++]);
            if (i == numberOfChunks)
                break;
            list4.add(chunks[i++]);
            if (i == numberOfChunks)
                break;
            list5.add(chunks[i]);
        }
        lists[0] = list1;
        lists[1] = list2;
        lists[2] = list3;
        lists[3] = list4;
        lists[4] = list5;

        for (int i = 1; i <= 5; i++) {
            Socket curSocket = listener.accept();
            fileThread thread = new fileThread(curSocket, i, lists[i - 1], numberOfChunks, file.getName());
            System.out.println("Give Files to peer" + i);
            thread.start();
        }
        Thread.sleep(1000000);
        System.out.println("All Files have been given to peers");
        System.out.println("Now closing the fileOwner...");
    }

    private static class fileThread extends Thread {
        private Socket peerSocket;
        private int peerNumber;
        private List<File> chunks;
        private int totalNum;
        private String fileName;
        private ObjectOutputStream output;
        public fileThread (Socket peerSocket, int peerNumber, List<File> chunks, int totalNum, String fileName) {
            this.peerSocket = peerSocket;
            this.peerNumber = peerNumber;
            this.chunks = chunks;
            this.totalNum = totalNum;
            this.fileName = fileName;
        }
        public void run() {
            try {
                output = new ObjectOutputStream(peerSocket.getOutputStream());
                sendMessage(fileName + "");
                sendMessage(totalNum + "");
                sendMessage(chunks.size() + "");
                for (File chunk : chunks) {
                    sendMessage(chunk.getName());
                    sendFile(chunk);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }

        }
        public void sendMessage(String message) {
            try {
                output.writeObject(message);
                output.flush();
            }
            catch(IOException ioException) {
                ioException.printStackTrace();
            }
        }

        public void sendFile(File file) {
            try {
                byte[] buffer = Files.readAllBytes(file.toPath());
                output.writeObject(buffer);
                output.flush();
                System.out.println("Send File: " + file.getName() + " to peer" + peerNumber);
            }
            catch(IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

}
