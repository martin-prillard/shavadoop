package fr.telecompt.shavadoop.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import fr.telecompt.shavadoop.master.thread.LaunchShufflingMap;
import fr.telecompt.shavadoop.master.thread.LaunchSplitMapping;
import fr.telecompt.shavadoop.master.thread.ReceiveSlaveInfo;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.PropReader;
import fr.telecompt.shavadoop.util.Util;

/**
 * Master object
 *
 */
public class Master
{
	private String fileToTreat = null;
	private String hostMaster = null;

	private int nbWorkerMax;
	private PropReader prop = new PropReader();
	private SSHManager sm;
	private int workersMapper;
	private int lifeTimeThread;
	
	// Map file and host
	private Map<String, String> filesHostMappers = new HashMap<String, String>();
	
	
	/**
	 * Clean and initialize the MapReduce process
	 */
	public void initialize(){
		
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Initialize and clean " + Constant.APP_DEBUG_TITLE);

		lifeTimeThread = Integer.parseInt(prop.getPropValues(PropReader.THREAD_LIFETIME));
		
		// create directory if not exist
		Util.createDirectory(new File(Constant.APP_PATH_REPO_RES));

		// create the shell manager
		sm = new SSHManager();
		
		// get values from properties file
		fileToTreat = prop.getPropValues(PropReader.FILE_INPUT);
		nbWorkerMax = Integer.parseInt(prop.getPropValues(PropReader.WORKER_MAX));
	

		try {
			hostMaster = InetAddress.getLocalHost().getHostName();
			Constant.USERNAME_MASTER = System.getProperty("user.name");
		} catch (Exception e) {e.printStackTrace();}
		
		// clean directory
		Util.cleanDirectory(new File(Constant.APP_PATH_REPO_RES)); 
	}
	
	/**
	 * Launch MapReduce process
	 */
	public void launchMapReduce() {
    	
		initialize();
		
		// get workers
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Get workers core alive : " + Constant.APP_DEBUG_TITLE);
		List<String> workersMapperCores = sm.getHostAliveCores(nbWorkerMax);
		if (Constant.APP_DEBUG) System.out.println("Workers core : " + workersMapperCores); 
		
        // Split the file
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Input splitting : " + Constant.APP_DEBUG_TITLE);
		List<String> filesToMap = inputSplitting(workersMapperCores, fileToTreat);
    	
        // Launch maps process
    	if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch map threads : " + Constant.APP_DEBUG_TITLE);
        Map<String, HashSet<String>> groupedDictionary = launchMapThreads(workersMapperCores, filesToMap);
        if (Constant.APP_DEBUG) System.out.println("Grouped dictionary : " + groupedDictionary);
        
        // Launch shuffling maps process
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch shuffling threads : " + Constant.APP_DEBUG_TITLE);
        Map<String, String> filesHostReducers = null;
		try {
			filesHostReducers = launchShufflingMapThreads(groupedDictionary);
		} catch (IOException e) {e.printStackTrace();}
        // Assembling final maps
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Assembling final maps : " + Constant.APP_DEBUG_TITLE);
        assemblingFinalMaps(filesHostReducers);
        
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " MapReduce process done " + Constant.APP_DEBUG_TITLE);
	}
	
    /**
     * Split the original file
     * @param originalFile
     */
    public List<String> inputSplitting(List<String> workers, String fileToTreat) {
    	
    	if (Constant.APP_DEBUG) System.out.println("Shavadoop workflow on : " + fileToTreat);
    	
    	List<String> filesToMap = new ArrayList<String>();
		 try {
             FileReader fic = new FileReader(fileToTreat);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             int nbFile = 0;
             
             // get the number of line of the file
             LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(fileToTreat)));
             lnr.skip(Long.MAX_VALUE);
             int totalLine = (lnr.getLineNumber() + 1);
             lnr.close();
             
             int nbLineByHost = totalLine;
             int restLineByHost = 0;

             workersMapper = workers.size();
             
             // if too more worker available for map process
             if (workersMapper > totalLine) {
            	 workersMapper = totalLine;
             }
             
             // The rest of the division for the last host
             restLineByHost = totalLine % workersMapper;
             // Calculate the number of lines for each host
             nbLineByHost = (totalLine - restLineByHost) / (workersMapper);

             
             if (Constant.APP_DEBUG) System.out.println("Nb workers mappers : " + (workersMapper) + " " + workers);
             if (Constant.APP_DEBUG) System.out.println("Nb line to tread : " + (totalLine));
             if (Constant.APP_DEBUG) System.out.println("Nb line by host mapper : " + (nbLineByHost));
             if (Constant.APP_DEBUG) System.out.println("Nb line for the last host mapper : " + (restLineByHost));
             
             // Content of the file
             List<String> content = new ArrayList<String>();
             
             while ((line = read.readLine()) != null) {
            	 // Add line by line to the content file
            	 content.add(Util.cleanText(line));
            	 // Write the complete file by block or if it's the end of the file
            	 if ((content.size() == nbLineByHost && nbFile < workersMapper - 1)
            			 || (content.size() == nbLineByHost + restLineByHost && nbFile == workersMapper - 1)) {
                	 //For each group of line, we write a new file
                	 ++nbFile;
                	 String fileToMap = Constant.F_SPLITING + nbFile;
                	 Util.writeFile(fileToMap, content);
                	 
                	 if (Constant.APP_DEBUG) System.out.println("Input file splited in : " + fileToMap);
                	 		
                	 //We save names of theses files in a list
                	 filesToMap.add(fileToMap);
                	 // Reset
                	 content = new ArrayList<String>();
            	 }
             }
             
             fic.close();
             read.close();   
         } catch (IOException e) {
             e.printStackTrace();
         }
		 
		 return filesToMap;
    }

    /**
     * Launch a thread to execute map on each distant computer
     * @param dsaKey
     * @param fileIpAdress
     * @return grouped dictionary
     */
    public Map<String, HashSet<String>> launchMapThreads(List<String> workersMapperCores, List<String> filesToMap) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		
		if (Constant.APP_DEBUG) System.out.println("Nb workers mappers : " + workersMapperCores.size());
		if (Constant.APP_DEBUG) System.out.println("Nb files splitted : " + filesToMap.size());
		
		//For each files to map
    	for (int i = 0; i < filesToMap.size(); i++) {
    		//We launch a map Thread to execute the map process on a computer
    		boolean local = false;
    		if (workersMapperCores.get(i).equalsIgnoreCase(sm.getHostMasterFull())) {
    			// the worker is the master
    			local = true;
    		}
			es.execute(new LaunchSplitMapping(local, sm.getDsaKey(), workersMapperCores.get(i), filesToMap.get(i), hostMaster));
       	 	//We save the name of the file and the mapper
        	filesHostMappers.put(filesToMap.get(i), workersMapperCores.get(i)); 
    	}
    	es.shutdown();
    	
        // Create dictionary
    	Map<String, HashSet<String>> groupedDictionary = null;
		try {
			groupedDictionary = createDictionary();
		} catch (IOException e) {e.printStackTrace();}
		
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(lifeTimeThread, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return groupedDictionary;
    }
    
    
    /**
     * Create a dictionary with key (word) and value (files names)
     * @return
     * @throws IOException
     */
    public Map<String, HashSet<String>> createDictionary() throws IOException {
    	
    	Map<String, HashSet<String>> dictionary = new HashMap<String, HashSet<String>>();
    	
		int port = Integer.parseInt(prop.getPropValues(PropReader.PORT_MASTER));
		
    	// Create dictionnary with socket
    	ServerSocket ss = new ServerSocket(port);
    	
    	// Threat to listen slaves info
    	ExecutorService es = Executors.newCachedThreadPool();

    	// While we haven't received all elements dictionary from the mappers
    	for (int i = 0; i < workersMapper; i++) {
			es.execute(new ReceiveSlaveInfo(ss, dictionary));
    	}
    	
    	es.shutdown();
    	
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(lifeTimeThread, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		ss.close();
		
        return dictionary;
    }
    
    
    /**
     * Launch a thread to execute shuffling map on each distant computer
     * @param dictionary
     * @throws IOException 
     */
    public Map<String, String> launchShufflingMapThreads(Map<String, HashSet<String>> dictionary) throws IOException {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		// Host who have a reduce file to assemble
		Map<String, String> filesHostReducers = new HashMap<String, String>();
		
		// get a list of workers reducer alive
		List<String> workersReducerCores = sm.getHostAliveCores(nbWorkerMax);
		
		int idWorkerReducerCore = 0;
		String workerReducer = workersReducerCores.get(idWorkerReducerCore);

		int workersReducer = workersReducerCores.size();
		int totalReducerTodo = dictionary.size();
		
        // if too more worker available for reduce process
        if (workersReducer > totalReducerTodo) {
        	workersReducer = totalReducerTodo;
        }
        // The rest of the division for the last host
        int restThreadByCore = totalReducerTodo % workersReducer;
        // Calculate the number of lines for each host
        int nbThreadByCore = (totalReducerTodo - restThreadByCore) / (workersReducer);
        
        if (Constant.APP_DEBUG) System.out.println("Nb workers core reducer : " + (workersReducer) + " " + workersReducerCores);
        if (Constant.APP_DEBUG) System.out.println("Nb reduce to do : " + (totalReducerTodo));
        if (Constant.APP_DEBUG) System.out.println("Nb reduce by core : " + (nbThreadByCore));
        if (Constant.APP_DEBUG) System.out.println("Nb reduce for the last core : " + (restThreadByCore));
        
		// the number of light threads by computer
		int cptLightThread = 0;
		
		// File output
		String shufflingDictionaryFile = Constant.F_SHUFFLING_DICTIONARY + Constant.F_SEPARATOR + idWorkerReducerCore;
		FileWriter fw = new FileWriter(shufflingDictionaryFile);
		BufferedWriter bw = new BufferedWriter(fw);
		PrintWriter write = new PrintWriter(bw); 
		
		//For each key and files to shuffling maps
		for (Entry<String, HashSet<String>> e : dictionary.entrySet()) {
			
			write.println(e.getKey()
			    		+ Constant.FILE_SEPARATOR
			    		+ Util.listToString(e.getValue())); 

			filesHostReducers.put(e.getKey(), workerReducer);
			++cptLightThread;
			
			if ((cptLightThread == nbThreadByCore && idWorkerReducerCore < workersReducer - 1)
					|| (cptLightThread == nbThreadByCore + restThreadByCore && idWorkerReducerCore == workersReducer - 1)) {
				write.close();
				// launch shuffling map process
	    		boolean local = false;
	    		if (workerReducer.equalsIgnoreCase(sm.getHostMasterFull())) {
	    			// the worker is the master
	    			local = true;
	    		}
				es.execute(new LaunchShufflingMap(local, sm.getDsaKey(), workerReducer, shufflingDictionaryFile, hostMaster));
				// if enought heavy threads launch for one distant computer, change the worker reducer
				++idWorkerReducerCore;
				// if still worker needed
				if (idWorkerReducerCore < workersReducerCores.size()) {
					workerReducer = workersReducerCores.get(idWorkerReducerCore);
					// reset 
					cptLightThread = 0;
					shufflingDictionaryFile = Constant.F_SHUFFLING_DICTIONARY + Constant.F_SEPARATOR + idWorkerReducerCore;
					fw = new FileWriter(shufflingDictionaryFile);
					bw = new BufferedWriter (fw);
					write = new PrintWriter(bw); 
				}
       	 	}
		}
		
		es.shutdown();
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(lifeTimeThread, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return filesHostReducers;
    }
    
    
    /**
     * Concat final maps together in one file result
     */
    public void assemblingFinalMaps(Map<String, String> filesHostReducers) {
    	// Final file to reduce
    	String fileFinalResult = Constant.F_FINAL_RESULT;
    	// Get the list of file
    	List<String> listFiles = new ArrayList<String>();
    	 
    	for (Entry<String, String> e : filesHostReducers.entrySet()) {
    		listFiles.add(Constant.F_REDUCING 
    				+ Constant.F_SEPARATOR 
    				+ e.getKey() // keyword
    				+ Constant.F_SEPARATOR 
    				+ e.getValue()); // hostname
    	}

    	if (Constant.APP_DEBUG) System.out.println("Files to merge : " + listFiles);
    	
    	// Concat data of each files in one
		try {
             Map<String, Integer> finalResult = new HashMap<String, Integer>();
             
             // For each files
			 for (int i = 0; i < listFiles.size(); i++) {
	             FileReader fic = new FileReader(listFiles.get(i));
	             BufferedReader read = new BufferedReader(fic);
	             String line = null;
	
	             // For each lines of the file
	             while ((line = read.readLine()) != null) {
		            String words[] = line.split(Constant.FILE_SEPARATOR);
		            // Add each line to our hashmap
		            finalResult.put(words[0], Integer.parseInt(words[1]));
		            
		            if (Constant.APP_DEBUG) System.out.println(words[0] + Constant.FILE_SEPARATOR + Integer.parseInt(words[1]));
	             } 
	        	 
	             fic.close();
	             read.close();   
			 }
			 
        	 Util.writeFile(fileFinalResult, finalResult);
        	 
         } catch (IOException e) {	
             e.printStackTrace();
         }
    }

}

