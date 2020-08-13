import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.*;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;

public class peer {
	public static int port;
	public static int neighborPort;
	public static int fileOwnerPort;
	public static String address = "localhost";
    private static Socket peerSocket;
    private static ObjectOutputStream output;
    private static ObjectInputStream input;
    private static String peerDir;
    private static File folder;
    private static ReentrantLock lock = new ReentrantLock();
    
    public static void main(String[] args) throws Exception{
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        ArrayList<String> chunkList = new ArrayList<String>();
    	String originalFileName = new String();
    	int totalNum = 0;
    	fileOwnerPort = Integer.parseInt(args[0]);
    	port = Integer.parseInt(args[1]);
    	neighborPort = Integer.parseInt(args[2]);
    	peerSocket = new Socket(address, fileOwnerPort);
        System.out.println("Connected to " + address + " in port " + fileOwnerPort);
        output = new ObjectOutputStream(peerSocket.getOutputStream());
        output.flush();
        input = new ObjectInputStream(peerSocket.getInputStream());

        try {
        	originalFileName = (String)input.readObject();
        	totalNum = Integer.parseInt((String)input.readObject());
        	for (int i = 0; i < totalNum; i++) {
        		chunkList.add("chunk"+ i + ":0");
        	}
        	
            int numberOfChunks = Integer.parseInt((String)input.readObject());
            for (int i = 0; i < numberOfChunks; i++) {
                String fileName = (String) input.readObject();
                File file = new File(fileName);
                byte[] buffer = (byte[]) input.readObject();
                Files.write(file.toPath(), buffer);
                int index = chunkList.indexOf(fileName + ":0");
                chunkList.set(index, fileName + ":1");
                System.out.println("File" + fileName + "received");
            }
            String str = null;
            for (int i = 0; i < chunkList.size(); i++) {
            	if(str == null)
            		str = chunkList.get(i) + "\r\n";
            	else
            		str += (chunkList.get(i) + "\r\n");
        	}
            lock.lock();
            writeFile(str);
            lock.unlock();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Received all chunks from fileOwner");
        peerServer serverThread = new peerServer();
        peerClient clientThread = new peerClient(originalFileName, totalNum, port);
        serverThread.start();
        clientThread.start();
    }
    
    private static class peerServer extends Thread {
        private Socket neighborSocket;
        private ObjectOutputStream output;
        private ObjectInputStream input;
        
        public void run() {
            try {
            	ServerSocket listener = new ServerSocket(port);
                Socket neighborSocket = listener.accept();
                output = new ObjectOutputStream(neighborSocket.getOutputStream());
                input = new ObjectInputStream(neighborSocket.getInputStream());

                int remotePort = Integer.parseInt((String)input.readObject());
                
                while(true) {
                    if(((String)input.readObject()).equals("RequestList")) {
                    	System.out.println("(As Server) receive request for list from peer at " + remotePort);
                    	lock.lock();
                    	ArrayList<chunk> chunkList = new ArrayList<chunk>();
                    	BufferedReader br = new BufferedReader(new FileReader(new File("chunklist.txt"))); 
                		String str; 
                		while ((str = br.readLine()) != null) {
                			String[] list = str.split(":");
                			chunk c = new chunk();
                			c.name = list[0];
                			c.own = Integer.parseInt(list[1]);
                			chunkList.add(c);
                		}	
                		lock.unlock();
                        sendList(chunkList);
                        System.out.println("(As Server) send list to peer at " + remotePort);
                    }
                    String chunkName = (String) input.readObject();
                    if(!chunkName.equals("null")) {
                    	System.out.println("(As Server) receive request for "+ chunkName +" from peer at " +remotePort);
                    	File file = new File(chunkName);
                        sendFile(file, remotePort);
                        System.out.println("(As Server) send "+ chunkName +" to peer at " + remotePort);
                    }
                    else
                    	System.out.println("(As Server) receive "+ chunkName +" from peer at " + remotePort);
                }
            }
            catch (EOFException eof) {
            	System.out.println("(As Server) Disconnected!");
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void sendFile(File file, int remotePort) {
            try {
                byte[] buffer = Files.readAllBytes(file.toPath());
                output.writeObject(buffer);
                output.flush();
                System.out.println("(As Server) Send File: " + file.getName() + " to peer at " + remotePort);
            }
            catch(IOException ioException) {
                ioException.printStackTrace();
            }
        }
        
        public void sendList(ArrayList<chunk> list) {
            try {
                output.writeObject(list);
                output.flush();
            }
            catch(IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
    
    private static class peerClient extends Thread {
        private Socket neighborSocket;
        private ObjectOutputStream output;
        private ObjectInputStream input;
        private String fileName;
        private int totalNum;
        private int port;
        
        public peerClient (String fileName, int totalNum, int port) {
            this.fileName = fileName;
            this.totalNum = totalNum;
            this.port = port;
        }

        public void run() {
            while (true) {
            	try {
                	neighborSocket = new Socket(address, neighborPort);
                    output = new ObjectOutputStream(neighborSocket.getOutputStream());
                    input = new ObjectInputStream(neighborSocket.getInputStream()); 
                    System.out.println("(As CLient) Connected to " + address + " in port " + neighborPort);
                    break;
                }
                catch(UnknownHostException unknownHost){
                    System.err.println("(As Client) You are trying to connect to an unknown host!");
                }
                catch(ConnectException e) {
                    System.err.println("(As Client) Waiting to connect to peer at " + neighborPort + "...");
                }
                catch(SocketException e) {
                    System.err.println("(As Client) Connect failed");
                }
                catch(NumberFormatException nf) {
                    System.err.println("(As Client) Please input correct port number");
                }
                catch(IOException ioException){
                    ioException.printStackTrace();
                }
            }
    		
    		try {
        		boolean getAll = true;
        		getAll = getAllYet();
        		sendMessage("" + port);
                
                while(!getAll) {
                	sendMessage("RequestList");
                	System.out.println("(As Client) send Request List to peer at " + neighborPort);
                    ArrayList<chunk> neighborChunkList = (ArrayList<chunk>) input.readObject();
                    System.out.println("(As Client) receive list from peer at " + neighborPort);
                    
                    lock.lock();
            		ArrayList<chunk> chunkList = new ArrayList<chunk>();
            		BufferedReader br = new BufferedReader(new FileReader(new File("chunklist.txt"))); 
            		String str;
            		while ((str = br.readLine()) != null) {
            			String[] list = str.split(":");
            			chunk c = new chunk();
            			c.name = list[0];
            			c.own = Integer.parseInt(list[1]);
            			chunkList.add(c);
            		}
            		lock.unlock();
            		
            		ArrayList<Integer> requestList = new ArrayList<Integer>();
            		for(int i = 0; i < chunkList.size(); i++) {
            			if(neighborChunkList.get(i).own == 1 && chunkList.get(i).own == 0){
            				requestList.add(i);
            			}
            		}
            		if(!requestList.isEmpty()) {
            			int max = requestList.size();
            			int num = (int)(Math.random() * max);
            			int x = requestList.get(num);
            			sendMessage(chunkList.get(x).name);
            			System.out.println("(As Client) send request for "+ chunkList.get(x).name +" to peer at " + neighborPort);
                        File file = new File(chunkList.get(x).name);
                        byte[] buffer = (byte[]) input.readObject();
                        System.out.println("(As Client) get "+ chunkList.get(x).name +" from peer at " + neighborPort);
                        Files.write(file.toPath(), buffer);
                        chunkList.get(x).own = 1;
            		}
            		else
            			sendMessage("null");
                    String str1 = null;
                    for (int i = 0; i < chunkList.size(); i++) {
                    	if(str1 == null)
                    		str1 = chunkList.get(i).name + ":" + chunkList.get(i).own + "\r\n";
                    	else
                    		str1 += (chunkList.get(i).name + ":" + chunkList.get(i).own + "\r\n");
                	}
                    lock.lock();
                    writeFile(str1);
                    getAll = getAllYet();
                    lock.unlock();
                }
                
        		if(getAll) {
        			String[] chunks = new String[totalNum];
            		for(int i = 0; i < totalNum; i++) {
            			chunks[i] = "chunk" + i;
            		}
                	mergeFiles(chunks, fileName);
        		}
    		}
            catch (EOFException eof) {
            	System.out.println("(As Client) Disconnected!");
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        public boolean getAllYet() {
            try {
                String str = null;
                BufferedReader br = new BufferedReader(new FileReader(new File("chunklist.txt"))); 
        		while ((str = br.readLine()) != null) {
        			String[] list = str.split(":");
        			if(Integer.parseInt(list[1]) == 0) {
        				return false;
        			}
        		}
        		return true;
            }
            catch (Exception e) {
                e.printStackTrace();
                return false;
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
    }
    
	public static void writeFile(String str) {
		try {
			FileWriter fileWriter = new FileWriter("chunklist.txt");
			fileWriter.write(str);
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static class chunk implements Serializable{
		private String name;
		private int own;
	}
	
	public static void mergeFiles(String[] chunks, String resultPath) {
        File[] files = new File[chunks.length];
        for (int i = 0; i < chunks.length; i ++) {
            files[i] = new File(chunks[i]);
        }
        File resultFile = new File(resultPath);
        try {
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(resultFile));
            byte[] buffer = new byte[1024];
            for (int i = 0; i < chunks.length; i ++) {
                BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(files[i]));
                int count;
                while ((count = inputStream.read(buffer)) > 0) {
                	byte[] bb = new byte[count];
                	for (int index = 0; index < count; index++)
                		bb[index] = buffer[index];
                    outputStream.write(bb, 0, count);
                }
                inputStream.close();
            }
            System.out.println(resultPath + " merged!");
            outputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
