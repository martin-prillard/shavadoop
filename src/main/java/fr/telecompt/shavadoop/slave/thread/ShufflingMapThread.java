package fr.telecompt.shavadoop.slave.thread;

import fr.telecompt.shavadoop.slave.Slave;

public class ShufflingMapThread extends Thread {

	private String key;
	private String filesToShuffling;
	private Slave slave;
	
	public ShufflingMapThread(Slave _slave, String _key, String _filesToShuffling) {
		key = _key;
		filesToShuffling = _filesToShuffling;
		slave = _slave;
	}
	
	public void run() {
		//Lanch shuffling map method
		String fileSortedMaps = slave.shufflingMaps(key, filesToShuffling);
		//Launch reduce method	
		slave.mappingSortedMaps(key, fileSortedMaps);
	}
}
