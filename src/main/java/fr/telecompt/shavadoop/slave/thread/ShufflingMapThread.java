package fr.telecompt.shavadoop.slave.thread;

import fr.telecompt.shavadoop.slave.Slave;

public class ShufflingMapThread extends Thread {

	private String key;
	private String filesToTreat;
	private Slave slave;
	
	public ShufflingMapThread(Slave _slave, String _key, String _filesToTreat) {
		key = _key;
		filesToTreat = _filesToTreat;
		slave = _slave;
	}
	
	public void run() {
		//Lanch shuffling map method
		String fileSortedMaps = slave.shufflingMaps(key, filesToTreat);
		//Launch reduce method	
		slave.mappingSortedMaps(key, fileSortedMaps);
	}
}
