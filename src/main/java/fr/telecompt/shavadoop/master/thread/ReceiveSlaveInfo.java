package fr.telecompt.shavadoop.master.thread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Map;

import fr.telecompt.shavadoop.util.Constant;

public class ReceiveSlaveInfo extends Thread {

	private Socket socket;
	private BufferedReader in = null;
	private Map<String, String> dictionary;
	
	public ReceiveSlaveInfo(Socket s, Map<String, String> _dictionary){
		 socket = s;
		 dictionary = _dictionary;
	}
	
	public void run() {
		try {
			// BufferedReader to read line by line
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			
			String message = null;
			
			// If the distant computer say it's done, all is sent
			while (!(message = in.readLine()).equals(Constant.SOCKET_END_MESSAGE)) {
	           String[] elements = message.split(Constant.SOCKET_SEPARATOR_MESSAGE);
	           // Add element dictionary in our dictionary
	           dictionary.put(elements[0], elements[1]);			
			}

	        in.close();

		} catch (IOException e) {e.printStackTrace();}
	}
	
}
