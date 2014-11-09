package fr.telecompt.shavadoop.master.thread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import fr.telecompt.shavadoop.util.Constant;

public class ReceiveSlaveInfo extends Thread {

	private ServerSocket ss;
	private BufferedReader in = null;
	private Map<String, HashSet<String>> dictionary;
	private Map<String, HashSet<String>> partDictionary = new HashMap<String, HashSet<String>>();
	
	public ReceiveSlaveInfo(ServerSocket _ss, Map<String, HashSet<String>> _dictionary){
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
	           concatToHashMap(partDictionary, elements[0], elements[1]);
	           
			}
			
			// concat the partDictionary with the dictionary
			for (Entry<String, HashSet<String>> e : partDictionary.entrySet()) {
				String word = e.getKey();
				HashSet<String> listFiles = e.getValue();
				if (dictionary.keySet().contains(word)) {
					dictionary.get(word).addAll(listFiles);
				} else {
					dictionary.put(word, listFiles);
				}
			}
			
			if (Constant.APP_DEBUG) System.out.println("Master received all dictionary elements from a slave");
			
	        in.close();

		} catch (IOException e) {e.printStackTrace();}
	}
	
	public void concatToHashMap(Map<String, HashSet<String>> map, String key, String value) {
        if (map.keySet().contains(key)) {
     	   map.get(key).add(value);
        } else {
        	HashSet<String> listValues = new HashSet<String>();
        	listValues.add(value);
        	map.put(key, listValues);
        }
	}
	
}