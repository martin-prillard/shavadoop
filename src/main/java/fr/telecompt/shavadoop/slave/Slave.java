package fr.telecompt.shavadoop.slave;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import fr.telecompt.shavadoop.network.FileTransfert;
import fr.telecompt.shavadoop.network.SSHManager;
import fr.telecompt.shavadoop.slave.tasktracker.StateSlave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.Pair;
import fr.telecompt.shavadoop.util.PropReader;
import fr.telecompt.shavadoop.util.Util;

/**
 * 
 * @author martin prillard
 *
 */
public class Slave {
	
	public final static String SPLIT_MAPPING_FUNCTION = "split_mapping_function";
	public final static String SHUFFLING_MAP_FUNCTION = "shuffling_map_function";
	
	private PropReader prop = new PropReader();
	private boolean taskFinished = false;
	private String functionName;
	private String hostMaster;
	private String fileToTreat;
	private SSHManager sm;
	private boolean state = true;
	private int portMasterDictionary;
	private int portTaskTracker;
	ConcurrentMap<String, Integer> finalMapsInMemory = new ConcurrentHashMap<String, Integer>();
	private String idWorker;
	private int nbWorker;
	
	
    public Slave(String _nbWorker, String _idWorker, String _hostMaster, String _functionName, String _fileToTreat) {
    	nbWorker = Integer.parseInt(_nbWorker);
    	idWorker = _idWorker;
    	hostMaster = _hostMaster;
    	functionName = _functionName;
    	fileToTreat = _fileToTreat;
    	portMasterDictionary = Integer.parseInt(prop.getPropValues(PropReader.PORT_MASTER_DICTIONARY));
    	portTaskTracker = Integer.parseInt(prop.getPropValues(PropReader.PORT_TASK_TRACKER));
    }
	
    
    /**
     * Execute a worker's task
     */
    public void launchTask() {
    	
		// initialize the SSH manager
    	sm = new SSHManager();
    	sm.initialize();
    	
    	if (Constant.TASK_TRACKER) {
	    	// launch thread slave state for the task tracker
	    	StateSlave sst = new StateSlave(this, hostMaster, portTaskTracker);
	    	sst.start();
    	}
    	
    	switch (functionName){
    	
	    	case SPLIT_MAPPING_FUNCTION:
	    		// launch map method
	    		splitMapping(nbWorker, hostMaster, fileToTreat);
	    		break;
	    		
	    	case SHUFFLING_MAP_FUNCTION:
	        	
	    		int threadMaxByWorker = Integer.parseInt(prop.getPropValues(PropReader.THREAD_MAX_BY_WORKER));
	    		int threadQueueMaxByWorker = Integer.parseInt(prop.getPropValues(PropReader.THREAD_QUEUE_MAX_BY_WORKER));
	    		
	    		// launch shuffling map thread
	    		launchShufflingMapThread(threadMaxByWorker, threadQueueMaxByWorker);
	    		
    			// write the RM file
    			String fileToAssemble = Constant.PATH_F_REDUCING 
           			 + Constant.SEP_NAME_FILE
           			 + idWorker
           			 + Constant.SEP_NAME_FILE 
           			 + sm.getHostFull();
    			Util.writeFile(fileToAssemble, finalMapsInMemory);
	    		
	    		// SLAVE file -> MASTER
    			ExecutorService esScpFile = Util.fixedThreadPoolWithQueueSize(threadMaxByWorker, threadQueueMaxByWorker);
    			esScpFile.execute(new FileTransfert(sm, hostMaster, fileToAssemble, fileToAssemble, true));
				esScpFile.shutdown();
	    		try {
	    			esScpFile.awaitTermination(Integer.parseInt(prop.getPropValues(PropReader.THREAD_MAX_LIFETIME)), TimeUnit.MINUTES);
	    		} catch (InterruptedException e) {
	    			e.printStackTrace();
	    			state = false;
	    		}
	    		break;
    	}
    	
    	// if no fail
    	if(state) {
    		taskFinished = true;
    	}

    }
    
    
    /**	
     * Map method
     * @param fileToMap
     */
    private void splitMapping(int nbWorker, String hostMaster, String fileToMap) {
		 try {
             FileReader fic = new FileReader(fileToMap);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             
             Map<String, Pair> partDictionary = new HashMap<String, Pair>();
             int idNextWorker = 0;
             
             List<HashMap<String, Integer>> unsortedMaps = new ArrayList<HashMap<String, Integer>>();
             for (int i = 0; i < nbWorker; i++) {
            	 unsortedMaps.add(new HashMap<String, Integer>());
             }
             
             while ((line = read.readLine()) != null) {
            	// clean the line
 			    line = Util.cleanLine(line);
 			    // don't write out blank lines
 			    if (!line.equals("") || !line.isEmpty()) {
 	            	 unsortedMaps = wordCount(nbWorker, unsortedMaps, line);
 			    }
             }
             
             fic.close();
             read.close();   

        	 for (HashMap<String, Integer> e : unsortedMaps) {
        		 if (!e.isEmpty()) {
	                 // Write UM File
	            	 String fileToShuffle = Constant.PATH_F_MAPPING 
	            			 + Constant.SEP_NAME_FILE 
	               			 + idWorker
	               			 + Constant.SEP_NAME_FILE
	               			 + Constant.F_MAPPING_BY_WORKER
	               			 + Constant.SEP_NAME_FILE
	               			 + idNextWorker
	               			 + Constant.SEP_NAME_FILE 
	               			 + sm.getHostFull();
	            	 
	        		 Util.writeFile(fileToShuffle, e);
	        		 partDictionary.put(String.valueOf(idNextWorker), new Pair(sm.getHostFull(), fileToShuffle));
	        		 ++idNextWorker;
        		 }
        	 }
        	 
        	 // send dictionary with UNIQUE key (word) and hostname to the master
        	 sendDictionaryElement(hostMaster, partDictionary);
        	 
         } catch (Exception e) {
             e.printStackTrace();
             state = false;
         }
    }

    
    /**
     * Count the occurence of each word in the sentence
     * @param line
     * @return res
     */
    private List<HashMap<String, Integer>> wordCount(int nbWorker, List<HashMap<String, Integer>> mapWc, String line) {
    	// split the line word by word
    	String words[] = line.split(Constant.SEP_WORD);
    	
    	for (int i = 0; i < words.length; i++) {
    		String word = words[i];
    		// add counter value for this word
    		int idNextWorker =  getIdNextWorker(word, nbWorker);
    		// increment like the hadoop combiner
    		if (mapWc.get(idNextWorker).containsKey(word)) {
    			int oldCounter = mapWc.get(idNextWorker).get(word);
    			mapWc.get(idNextWorker).put(word, ++oldCounter);
    		} else {
    			mapWc.get(idNextWorker).put(word, 1);
    		}
    	}
    	
    	return mapWc;
    }
    
    
    /**
     * Return the id next worker from the key
     * @param key
     * @param nbWorker
     * @return id next worker
     */
    private int getIdNextWorker(String key, int nbWorker) {
    	return Math.abs((int) (Util.simpleHash(key) % nbWorker));
    }
    
    
    /**
     * Send to the master the id next worker and the names of files to do treat by the next worker
     * @param hostMaster
     * @param partDictionary
     * @throws UnknownHostException
     * @throws IOException
     */
    private void sendDictionaryElement(String hostMaster, Map<String, Pair> partDictionary) throws UnknownHostException, IOException {
        Socket socket = new Socket(hostMaster, portMasterDictionary);
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        
        // send dictionary element
        out.writeObject(partDictionary);
        out.flush();
        out.close();
        socket.close();
    }
    
    
    /**
     * Launch shuffling map process for each UM files in the DSM file
     */
    private void launchShufflingMapThread(int threadMaxByWorker, int threadQueueMaxByWorker) {
		ExecutorService es = Util.fixedThreadPoolWithQueueSize(threadMaxByWorker, threadQueueMaxByWorker);
		
		try{
		    InputStream ips = new FileInputStream(fileToTreat); 
			InputStreamReader ipsr = new InputStreamReader(ips);
			BufferedReader br = new BufferedReader(ipsr);
			String shufflingDictionaryLine;
			
			// for each UM files in the DSM file
			while((shufflingDictionaryLine = br.readLine()) != null){
				String[] elements = shufflingDictionaryLine.split(Constant.SEP_CONTAINS_FILE);
				String host = elements[0];
				String fileToShuffling = elements[1];
				// excute the shuffling map on it
				es.execute(new ShufflingMapThread(sm, this, finalMapsInMemory, host, fileToShuffling));
			}
			
			br.close();
			ipsr.close();
			br.close();
			
			es.shutdown();
			try {
				es.awaitTermination(Integer.parseInt(prop.getPropValues(PropReader.THREAD_MAX_LIFETIME)), TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				e.printStackTrace();
				state = false;
			}
		} catch (IOException e) {
			System.out.println("No shuffling dictionary file : " + fileToTreat);
			state = false;
		}
    }
    
    
	public boolean isState() {
		return state;
	}

	
	public void setState(boolean state) {
		this.state = state;
	}

	
	public boolean isTaskFinished() {
		return taskFinished;
	}
	
}
