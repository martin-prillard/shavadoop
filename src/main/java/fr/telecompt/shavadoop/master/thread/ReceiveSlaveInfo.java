package fr.telecompt.shavadoop.master.thread;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.Pair;

public class ReceiveSlaveInfo extends Thread {

	private ServerSocket ss;
	private Map<String, HashSet<Pair>> dictionary;
	private Map<String, HashSet<Pair>> partDictionary = new HashMap<String, HashSet<Pair>>();
	
	public ReceiveSlaveInfo(ServerSocket _ss, Map<String, HashSet<Pair>> _dictionary){
		 ss = _ss;
		 dictionary = _dictionary;
	}
	
	public void run() {
		try {
			Socket socket = ss.accept();
			// BufferedReader to read line by line
			ObjectInputStream objectInput = new ObjectInputStream(socket.getInputStream()); 
			
            try {
                Object object = objectInput.readObject();
                if (object instanceof HashMap<?, ?>) {
                	@SuppressWarnings("unchecked")
                	Map<String, Pair> pd =  (HashMap<String, Pair>) object;
                	for (Entry<String, Pair> e : pd.entrySet()) {
                	    // Add element dictionary in our dictionary
                		Pair p = e.getValue();
         	            concatToHashMap(partDictionary, e.getKey(), p.getVal1(), p.getVal2());
                	}
                }
                objectInput.close();
            } catch (Exception e) {e.printStackTrace();}
			
            objectInput.close();
            
			// concat the partDictionary with the dictionary
			for (Entry<String, HashSet<Pair>> e : partDictionary.entrySet()) {
				String word = e.getKey();
				HashSet<Pair> listFilesCaps = e.getValue();
				if (dictionary.keySet().contains(word)) {
					dictionary.get(word).addAll(listFilesCaps);
				} else {
					dictionary.put(word, listFilesCaps);
				}
			}
			
			String hostClient = socket.getRemoteSocketAddress().toString();
			if (Constant.MODE_DEBUG) System.out.println("Master received all dictionary elements from " + hostClient);

		} catch (IOException e) {e.printStackTrace();}
	}
	
	public void concatToHashMap(Map<String, HashSet<Pair>> map, String key, String hostOwner, String value) {
        if (map.keySet().contains(key)) {
     	    map.get(key).add(new Pair(hostOwner, value));
        } else {
        	HashSet<Pair> listFilesCaps = new HashSet<Pair>();
        	listFilesCaps.add(new Pair(hostOwner, value));
        	map.put(key, listFilesCaps);
        }
	}
	
}
