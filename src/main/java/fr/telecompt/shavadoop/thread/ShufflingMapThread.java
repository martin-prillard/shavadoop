package fr.telecompt.shavadoop.thread;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.Pair;

public class ShufflingMapThread extends Thread {

	private String key;
	private String filesToShuffling;
	private Slave slave;
	private SSHManager sm;
	
	public ShufflingMapThread(SSHManager _sm, Slave _slave, String _key, String _filesToShuffling) {
		key = _key;
		filesToShuffling = _filesToShuffling;
		slave = _slave;
		sm = _sm;
	}
	
	public void run() {
		
    	// Get the list of file
    	String[] listFilesCaps = filesToShuffling.split(Constant.SEP_FILES_SHUFFLING_MAP_GROUP);
    	String[] listFilesToTreat = new String[listFilesCaps.length];
    	
    	for (int i = 0; i < listFilesCaps.length; i++) {
    		String[] fileCaps = listFilesCaps[i].split(Constant.SEP_FILES_SHUFFLING_MAP);
	    	String fileToTreat = fileCaps[1];
    		listFilesToTreat[i] = fileToTreat;
    	}
    	
    	if (Constant.MODE_SCP_FILES) {
			// SLAVE <- SLAVE/MASTER files
	    	ExecutorService es = Executors.newCachedThreadPool();
	    	
	    	// For each files
	    	for (int i = 0; i < listFilesCaps.length; i++) {
		    	String[] fileCaps = listFilesCaps[i].split(Constant.SEP_FILES_SHUFFLING_MAP);
		    	String hostOwner = fileCaps[0];
		    	String fileToTreat = fileCaps[1];
				String destFile = Constant.PATH_SLAVE + FilenameUtils.getBaseName(fileToTreat);
				// if the file doesn't exist on this computer
				File f = new File(fileToTreat);
				if (!f.exists()) {
					es.execute(new FileTransfert(sm, hostOwner, fileToTreat, destFile));
					listFilesToTreat[i] = destFile;
				}
	    	}
	    	es.shutdown();
			try {
				es.awaitTermination(Constant.THREAD_MAX_LIFETIME, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				e.printStackTrace();
				slave.setState(false);
			}
    	}
		
		//Lanch shuffling map method
		List<Pair> sortedMaps = slave.shufflingMaps(key, listFilesToTreat);
		//Launch reduce method
		slave.mappingSortedMapsInMemory(key, sortedMaps);

	}
}
