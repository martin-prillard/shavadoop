package fr.telecompt.shavadoop.master.thread;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.Pair;

public class ListenerSlaves extends Thread {

	private int portMaster;
	private int nbWorkerMappers;
	private Map<String, HashSet<Pair>> dictionary;
	
	public ListenerSlaves(int _portMaster, int _nbWorkerMappers, Map<String, HashSet<Pair>> _dictionary) {
		portMaster = _portMaster;
		nbWorkerMappers = _nbWorkerMappers;
		dictionary = _dictionary;
	}
	
	public void run() {
		try {
			// Create dictionnary with socket
			ServerSocket ss = new ServerSocket(portMaster);

	    	// Threat to listen slaves info
	    	ExecutorService es = Executors.newCachedThreadPool();
	    	
	    	// While we haven't received all elements dictionary from the mappers
	    	for (int i = 0; i < nbWorkerMappers; i++) {
				es.execute(new ReceiveSlaveInfo(ss, dictionary));
	    	}
	    	es.shutdown();
	    	
			//Wait while all the threads are not finished yet
			try {
				es.awaitTermination(Constant.THREAD_LIFETIME, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			ss.close();
		} catch (IOException e1) {e1.printStackTrace();}
	}
}
