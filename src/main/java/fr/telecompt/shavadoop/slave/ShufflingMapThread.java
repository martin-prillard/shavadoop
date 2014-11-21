package fr.telecompt.shavadoop.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;

import fr.telecompt.shavadoop.network.FileTransfert;
import fr.telecompt.shavadoop.network.SSHManager;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.Util;

/**
 * 
 * @author martin prillard
 *
 */
public class ShufflingMapThread extends Thread {

	private String hostOwner;
	private String fileToShuffling;
	private Slave slave;
	private SSHManager sm;
	private volatile ConcurrentHashMap<String, List<Integer>> sortedMaps;
	
	public ShufflingMapThread(SSHManager _sm, Slave _slave, ConcurrentHashMap<String, List<Integer>> _sortedMaps, String _hostOwner, String _fileToShuffling) {
		hostOwner = _hostOwner;
		fileToShuffling = _fileToShuffling;
		slave = _slave;
		sortedMaps = _sortedMaps;
		sm = _sm;
	}
	
	
	public void run() {
		
		// SLAVE <- SLAVE/MASTER files
    	ExecutorService es = Executors.newCachedThreadPool();

		String destFile = Constant.PATH_REPO_RES 
				+ FilenameUtils.getName(fileToShuffling);
		
		// if the file doesn't exist on this computer
		File f = new File(fileToShuffling);
		if (!f.exists()) {
			es.execute(new FileTransfert(sm, hostOwner, fileToShuffling, destFile, false, false));
			fileToShuffling = destFile;
		}
			
    	es.shutdown();
		try {
			es.awaitTermination(Constant.THREAD_MAX_LIFETIME, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
			slave.setState(false);
		}
		
    	//Lanch reduce method
		shufflingMaps(fileToShuffling);
	}
	
    /**
     * Group and sort maps results by key
     * @param file
     * @return sorted maps
     */
    public void shufflingMaps(String file) {
    	// concat data of each files in one list
    	try {
             FileReader fic = new FileReader(file);
             BufferedReader read = new BufferedReader(fic);
             String line = null;

             // For each lines of the file
             while ((line = read.readLine()) != null) {
	            String words[] = line.split(Constant.SEP_CONTAINS_FILE);
	            String word = words[0];
	            int counter = Integer.parseInt(words[1]);
	            sortedMaps.putIfAbsent(word, new ArrayList<Integer>()); //TODO see
 				sortedMaps.get(word).add(counter);
             } 
             fic.close();
             read.close();   
             
         } catch (Exception e) {
        	 List<String> r = new ArrayList<String>(); //TODO
             e.printStackTrace();
             r.add("erreur : " + e + " on " + sm.getHostFull()); //TODO
             Util.writeFile("/cal/homes/prillard/err.log", r); //TODO
           slave.setState(false);
         }
    }
	
}


