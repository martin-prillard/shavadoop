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

	private String hostOwner;
	private String fileToShuffling;
	private Slave slave;
	private SSHManager sm;
	
	public ShufflingMapThread(SSHManager _sm, Slave _slave, String _hostOwner, String _fileToShuffling) {
		hostOwner = _hostOwner;
		fileToShuffling = _fileToShuffling;
		slave = _slave;
		sm = _sm;
	}
	
	public void run() {
    	
    	if (Constant.MODE_SCP_FILES) {
			// SLAVE <- SLAVE/MASTER files
	    	ExecutorService es = Executors.newCachedThreadPool();

			String destFile = Constant.PATH_SLAVE + FilenameUtils.getBaseName(fileToShuffling);
			// if the file doesn't exist on this computer
			File f = new File(fileToShuffling);
			if (!f.exists()) {
				es.execute(new FileTransfert(sm, hostOwner, fileToShuffling, destFile));
				fileToShuffling = destFile;
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
		List<Pair> sortedMaps = slave.shufflingMaps(fileToShuffling);
		//Launch reduce method
		slave.mappingSortedMapsInMemory(sortedMaps);

	}
}
