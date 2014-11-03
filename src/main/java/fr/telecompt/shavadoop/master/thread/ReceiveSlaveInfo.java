package fr.telecompt.shavadoop.master.thread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;

import fr.telecompt.shavadoop.util.Constant;

public class ReceiveSlaveInfo extends Thread {

	private ServerSocket ss;
	private BufferedReader in = null;
	private Map<String, ArrayList<String>> dictionary;
	
	public ReceiveSlaveInfo(ServerSocket _ss, Map<String, ArrayList<String>> _dictionary){
		 ss = _ss;
		 dictionary = _dictionary;
	}
	
	public void run() {
		try {
			Socket socket = ss.accept();
			// BufferedReader to read line by line
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			
			String message = null;
			
			// If the distant computer say it's done, all is sent
			while (!(message = in.readLine()).equals(Constant.SOCKET_END_MESSAGE)) {
	           String[] elements = message.split(Constant.SOCKET_SEPARATOR_MESSAGE);
	           // Add element dictionary in our dictionary
	           
	           ArrayList<String> filesNames;
	           if (dictionary.keySet().contains(elements[0])) {
	        	   filesNames = dictionary.get(elements[0]);
	           } else {
	        	   filesNames = new ArrayList<String>();
	           }
        	   filesNames.add(elements[1]);
        	   dictionary.put(elements[0], filesNames);
	           
//	           System.out.println(">>> dictionary receive : " + elements[0] + " " + elements[1]);
        	   
			}

	        in.close();

		} catch (IOException e) {e.printStackTrace();}
	}
	
}
