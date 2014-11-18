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
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.tasktracker.StateSlave;
import fr.telecompt.shavadoop.thread.ShufflingMapThread;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.Pair;
import fr.telecompt.shavadoop.util.PropReader;
import fr.telecompt.shavadoop.util.Util;

/**
 * Slave object
 *
 */
public class Slave 
{
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
	// for mode all in one file is enable (best performance)
	Map<String, Integer> finalMapsInMemory = new HashMap<String, Integer>();
	private String idWorker;
	private int nbWorker;
	
	public Slave(){}
	
    public Slave(SSHManager _sm, String _nbWorker, String _idWorker, String _hostMaster, String _functionName, String _fileToTreat) {
    	sm = _sm;
    	nbWorker = Integer.parseInt(_nbWorker);
    	idWorker = _idWorker;
    	hostMaster = _hostMaster;
    	functionName = _functionName;
    	fileToTreat = _fileToTreat;
    	portMasterDictionary = Integer.parseInt(prop.getPropValues(PropReader.PORT_MASTER_DICTIONARY));
    	portTaskTracker = Integer.parseInt(prop.getPropValues(PropReader.PORT_TASK_TRACKER));
    }
	
    public void launchWork() {
    	
    	if (Constant.TASK_TRACKER) {
	    	// launch thread slave state for the task tracker
	    	StateSlave sst = new StateSlave(this, hostMaster, portTaskTracker);
	    	sst.start();
    	}
    	
    	switch (functionName){
    	
	    	case SPLIT_MAPPING_FUNCTION:
	    		//Launch map method
	    		splitMapping(nbWorker, hostMaster, fileToTreat);
	    		break;
	    		
	    	case SHUFFLING_MAP_FUNCTION:
	        	
	    		// launch shuffling maps process
	    		int threadMaxByWorker = Integer.parseInt(prop.getPropValues(PropReader.THREAD_MAX_BY_WORKER));
	    		int threadQueueMaxByWorker = Integer.parseInt(prop.getPropValues(PropReader.THREAD_QUEUE_MAX_BY_WORKER));
	    		
	    		ExecutorService es = Util.fixedThreadPoolWithQueueSize(threadMaxByWorker, threadQueueMaxByWorker);
	    		
	    		try {
    			    InputStream ips = new FileInputStream(fileToTreat); 
	    			InputStreamReader ipsr = new InputStreamReader(ips);
	    			BufferedReader br = new BufferedReader(ipsr);
	    			String shufflingDictionaryLine;
	    			
	    			while((shufflingDictionaryLine = br.readLine()) != null){
	    				String[] elements = shufflingDictionaryLine.split(Constant.SEP_CONTAINS_FILE);
	    				String host = elements[0];
	    				String fileToShuffling = elements[1];
	    				es.execute(new ShufflingMapThread(sm, this, host, fileToShuffling));
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
		    		
	    			String fileToAssemble = Constant.PATH_F_REDUCING 
	           			 + Constant.SEP_NAME_FILE
	           			 + sm.getHostFull()
	           			 + Constant.SEP_NAME_FILE 
	           			 + idWorker;
	    			Util.writeFile(fileToAssemble, finalMapsInMemory);
		    		
	    		} catch (IOException e) {
	    			System.out.println("No shuffling dictionary file");
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
    public void splitMapping(int nbWorker, String hostMaster, String fileToMap) {
		 try {
             FileReader fic = new FileReader(fileToMap);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             
             Map<String, Pair> partDictionary = new HashMap<String, Pair>();
             int idNextWorker = 0;
             
             List<List<Pair>> unsortedMaps = new ArrayList<List<Pair>>();
             for (int i = 0; i < nbWorker; i++) {
            	 unsortedMaps.add(new ArrayList<Pair>());
             }
             
             
             while ((line = read.readLine()) != null) {
            	 unsortedMaps = wordCount(nbWorker, unsortedMaps, line);
             }
             
             fic.close();
             read.close();   

             
        	 for (List<Pair> e : unsortedMaps) {
        		 if (!e.isEmpty()) {
	                 // Write UM File
	            	 String fileToShuffle = Constant.PATH_F_MAPPING 
	            			 + Constant.SEP_NAME_FILE 
	            			 + sm.getHostFull()
	            			 + Constant.SEP_NAME_FILE 
	               			 + idWorker
	               			 + Constant.SEP_NAME_FILE
	               			 + Constant.F_MAPPING_BY_WORKER
	               			 + Constant.SEP_NAME_FILE
	               			 + idNextWorker;
	            	 
	        		 Util.writeFileFromPair(fileToShuffle, e);
	        		 partDictionary.put(String.valueOf(idNextWorker), new Pair(sm.getHostFull(), fileToShuffle));
	        		 ++idNextWorker;
        		 }
        	 }
        	 
        	 // Send dictionary with UNIQUE key (word) and hostname to the master
        	 sendDictionaryElement(hostMaster, partDictionary);
        	 
         } catch (Exception e) {
             e.printStackTrace();
             state = false;
         }
    }

    
    /**
     * Count the occurence of each word in the sentence
     * @param line
     * @return
     */
    public List<List<Pair>> wordCount(int nbWorker, List<List<Pair>> mapWc, String line) {
    	//We split the line word by word
    	String words[] = line.split(Constant.SEP_WORD);
    	
    	for (int i = 0; i < words.length; i++) {
    		String word = words[i];
    		//Add counter value for this word
    		int idNextWorker =  getIdNextWorker(word, nbWorker);
    		mapWc.get(idNextWorker).add(new Pair(word, String.valueOf(1)));
    	}
    	
    	return mapWc;
    }
    
    /**
     * Return the id next worker from the key
     * @param key
     * @param nbWorker
     * @return
     */
    public int getIdNextWorker(String key, int nbWorker) {
    	return Math.abs((int) (Util.hash64(key) % nbWorker));
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
        
        // Send dictionary element
        out.writeObject(partDictionary);
        out.flush();
        out.close();
        socket.close();
    }
    
    
    /**
     * Group and sort maps results by key
     * @param file
     * @return
     */
    public List<Pair> shufflingMaps(String file) {
    	List<Pair> sortedMaps = new ArrayList<Pair>();
    	
    	// Concat data of each files in one list pair
		 try {
				 
             FileReader fic = new FileReader(file);
             BufferedReader read = new BufferedReader(fic);
             String line = null;

             // For each lines of the file
             while ((line = read.readLine()) != null) {
	            String words[] = line.split(Constant.SEP_CONTAINS_FILE);
            	sortedMaps.add(new Pair(words[0], words[1]));
             } 
             fic.close();
             read.close();   
	             
         } catch (Exception e) {
             e.printStackTrace();
             state = false;
         }
    	return sortedMaps;
    }
   
    
    /**
     * Reduce method in-memory
     * @param fileToReduce
     */
    public void mappingSortedMapsInMemory(List<Pair> sortedMaps) {
		 try {

             Map<String, Integer> localFinalMaps = new HashMap<String, Integer>();
             
             for (Pair p : sortedMaps) {
            	 String word = p.getVal1();
            	 String counter = p.getVal2();
            	// increment counter value for this word
          		if (localFinalMaps.containsKey(word)) {
          			localFinalMaps.put(word, localFinalMaps.get(word) + Integer.parseInt(counter));
          		} else {
          			localFinalMaps.put(word, Integer.parseInt(counter));
          		}
             }
    		 
 			// concat the localFinalMaps with the finalMapsInMemory
 			for (Entry<String, Integer> e : localFinalMaps.entrySet()) {
 				String word = e.getKey();
 				int counter = e.getValue();
 				if (finalMapsInMemory.keySet().contains(word)) {
 					finalMapsInMemory.put(word, finalMapsInMemory.get(word) + counter);
 				} else {
 					finalMapsInMemory.put(word, counter);
 				}
 			}
             
         } catch (Exception e) {
             e.printStackTrace();
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
