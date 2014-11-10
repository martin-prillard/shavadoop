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
import fr.telecompt.shavadoop.master.thread.ListenerSlaves;
import fr.telecompt.shavadoop.master.thread.TaskTracker;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.PropReader;
import fr.telecompt.shavadoop.util.Util;

/**
 * Master object
 *
 */
public class Master
{
	private String fileInputCleaned = null;
	private String hostMaster = null;
	private int portMaster;
	private int nbWorkerMax;
	private PropReader prop = new PropReader();
	private SSHManager sm;
	private int nbWorkerMappers;
	private double startTime;
	
	public Master(){
		startTime = System.currentTimeMillis();
	}
	
	/**
	 * Clean and initialize the MapReduce process
	 */
	public void initialize(){
		
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Initialize and clean " + Constant.APP_DEBUG_TITLE);

		// create the shell manager
		sm = new SSHManager();
		sm.initialize();
		if (Constant.APP_DEBUG) System.out.println("Shell manager initialized");
		
		// get values from properties file
		String fileToTreat = prop.getPropValues(PropReader.FILE_INPUT);
		nbWorkerMax = Integer.parseInt(prop.getPropValues(PropReader.WORKER_MAX));
		Constant.THREAD_LIFETIME = Integer.parseInt(prop.getPropValues(PropReader.THREAD_LIFETIME));
		portMaster = Integer.parseInt(prop.getPropValues(PropReader.PORT_MASTER));
		try {
			hostMaster = InetAddress.getLocalHost().getHostName();
			Constant.USERNAME_MASTER = System.getProperty("user.name");
		} catch (Exception e) {e.printStackTrace();}
		if (Constant.APP_DEBUG) System.out.println("Variables initialized");
		
		// clean directory
		Util.createDirectory(new File(Constant.APP_PATH_REPO_RES));
		Util.cleanDirectory(new File(Constant.APP_PATH_REPO_RES)); 
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_PATH_REPO_RES + " directory cleaned");
		
		// clean the input text
		fileInputCleaned = Constant.F_INPUT_CLEANED;
		Util.cleanText(fileToTreat, fileInputCleaned);
		if (Constant.APP_DEBUG) System.out.println("Input file cleaned");
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
		List<String> filesToMap = inputSplitting(workersMapperCores, fileInputCleaned);
    	
        // Launch maps process
    	if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch map threads : " + Constant.APP_DEBUG_TITLE);
        Map<String, HashSet<String>> groupedDictionary = launchMapThreads(workersMapperCores, filesToMap);
        if (Constant.APP_DEBUG) System.out.println("Grouped dictionary's size : " + groupedDictionary.size());
        
        // Launch shuffling maps process
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch shuffling threads : " + Constant.APP_DEBUG_TITLE);
        Map<String, String> filesHostReducers = null;
		try {
			filesHostReducers = launchShufflingMapThreads(groupedDictionary);
		} catch (IOException e) {e.printStackTrace();}
        // Assembling final maps
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Assembling final maps : " + Constant.APP_DEBUG_TITLE);
        assemblingFinalMaps(filesHostReducers);
        
        double totalTime = (System.currentTimeMillis() - startTime);
        totalTime = (double) ((totalTime / (1000.0*60.0)) % 60.0);
        
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " MapReduce process done in " + totalTime + " minutes " + Constant.APP_DEBUG_TITLE);
	}
	
    /**
     * Split the original file
     * @param originalFile
     */
    public List<String> inputSplitting(List<String> workers, String fileToTreat) {
    	
    	if (Constant.APP_DEBUG) System.out.println("Shavadoop workflow on : " + fileToTreat);
    	
    	List<String> filesToMap = new ArrayList<String>();
		 try {
             String line = null;
             int nbFile = 0;
             
             // get the number of line of the file
             FileReader fic = new FileReader(new File(fileToTreat));
             LineNumberReader  lnr = new LineNumberReader(fic);
             lnr.skip(Long.MAX_VALUE);
             int totalLine = (lnr.getLineNumber());
             lnr.close();
             
             int nbLineByHost = totalLine;
             int restLineByHost = 0;

             nbWorkerMappers = workers.size();
             
             // if too more worker available for map process
             if (nbWorkerMappers > totalLine) {
            	 nbWorkerMappers = totalLine;
             }
             
             // The rest of the division for the last host
             restLineByHost = totalLine % nbWorkerMappers;
             // Calculate the number of lines for each host
             nbLineByHost = (totalLine - restLineByHost) / (nbWorkerMappers);

             
             if (Constant.APP_DEBUG) System.out.println("Nb workers mappers : " + (nbWorkerMappers) + " " + workers);
             if (Constant.APP_DEBUG) System.out.println("Nb line to tread : " + (totalLine));
             if (Constant.APP_DEBUG) System.out.println("Nb line by host mapper : " + (nbLineByHost));
             if (Constant.APP_DEBUG) System.out.println("Nb line for the last host mapper : " + (restLineByHost));
             
             // Content of the file
             List<String> content = new ArrayList<String>();
             
             fic = new FileReader(new File(fileToTreat));
             BufferedReader read = new BufferedReader(fic);
             
             while ((line = read.readLine()) != null) {
            	 // Add line by line to the content file
            	 content.add(line);
            	 // Write the complete file by block or if it's the end of the file
            	 if ((content.size() == nbLineByHost && nbFile < nbWorkerMappers - 1)
            			 || (content.size() == nbLineByHost + restLineByHost && nbFile == nbWorkerMappers - 1)) {
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
             
             read.close();
             fic.close();
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
		TaskTracker ts = new TaskTracker(sm, hostMaster, es, null);
		
		if (Constant.APP_DEBUG) System.out.println("Nb workers mappers : " + workersMapperCores.size());
		if (Constant.APP_DEBUG) System.out.println("Nb files splitted : " + filesToMap.size());
		
		// dictionary
    	Map<String, HashSet<String>> groupedDictionary = new HashMap<String, HashSet<String>>();
    	// listener to get part dictionary from the worker mappers
    	es.execute(new ListenerSlaves(portMaster, nbWorkerMappers, groupedDictionary));
    	
		//For each files to map
    	for (int i = 0; i < filesToMap.size(); i++) {
	    	try {
	    	    Thread.sleep(250); // down the speed, use to not interfer with the listener slave thread
	    	} catch(InterruptedException ex) {
	    	    Thread.currentThread().interrupt();
	    	}
			es.execute(new LaunchSplitMapping(sm.isLocal(workersMapperCores.get(i)), sm.getDsaKey(), workersMapperCores.get(i), filesToMap.get(i), hostMaster));
			ts.addTask(workersMapperCores.get(i), Slave.SPLIT_MAPPING_FUNCTION, filesToMap.get(i), null);
    	}
    	
    	if (Constant.APP_DEBUG) System.out.println("Waitting the end of maps process...");
    	
    	ts.start();
    	
    	es.shutdown();

		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(Constant.THREAD_LIFETIME, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return groupedDictionary;
    }
    
    /**
     * Launch a thread to execute shuffling map on each distant computer
     * @param dictionary
     * @throws IOException 
     */
    public Map<String, String> launchShufflingMapThreads(Map<String, HashSet<String>> dictionary) throws IOException {
		// Host who have a reduce file to assemble
		Map<String, String> filesHostReducers = new HashMap<String, String>();
		
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		TaskTracker ts = new TaskTracker(sm, hostMaster, es, filesHostReducers);
		
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
        
        if (Constant.APP_DEBUG) System.out.println("Nb workers core reducer : " + workersReducer + " " + workersReducerCores);
        if (Constant.APP_DEBUG) System.out.println("Nb reduce to do : " + totalReducerTodo);
        if (Constant.APP_DEBUG) System.out.println("Nb reduce by core : " + nbThreadByCore);
        if (Constant.APP_DEBUG) System.out.println("Nb reduce for the last core : " + restThreadByCore);
        
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
				es.execute(new LaunchShufflingMap(sm.isLocal(workerReducer), sm.getDsaKey(), workerReducer, shufflingDictionaryFile, hostMaster));
				ts.addTask(workerReducer, Slave.SHUFFLING_MAP_FUNCTION, shufflingDictionaryFile, e.getKey());
				// if enought heavy threads launch for one distant computer, change the worker reducer
				++idWorkerReducerCore;
				// if still worker needed
				if (idWorkerReducerCore < workersReducerCores.size()) {
					workerReducer = workersReducerCores.get(idWorkerReducerCore);
					// reset 
					cptLightThread = 0;
					shufflingDictionaryFile = Constant.F_SHUFFLING_DICTIONARY + Constant.F_SEPARATOR + idWorkerReducerCore;
					fw = new FileWriter(shufflingDictionaryFile);
					bw = new BufferedWriter(fw);
					write = new PrintWriter(bw); 
				}
       	 	}
		}
		
		if (Constant.APP_DEBUG) System.out.println("Waitting the end of shuffling maps process...");
		
		fw.close();
		bw.close();
		write.close();
		
		ts.start();
		
		es.shutdown();
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(Constant.THREAD_LIFETIME, TimeUnit.MINUTES);
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
    		String nameFileToMerge = Constant.F_REDUCING 
    				+ Constant.F_SEPARATOR 
    				+ e.getKey() // keyword
    				+ Constant.F_SEPARATOR 
    				+ e.getValue(); // hostname
    		listFiles.add(nameFileToMerge); 
    	}

    	if (Constant.APP_DEBUG) System.out.println("Nb files to merge : " + listFiles.size());
    	
    	// Concat data of each files in one
		try {
             Map<String, Integer> finalResult = new HashMap<String, Integer>();
             
             // For each files
			 for (int i = 0; i < listFiles.size(); i++) {
				 File f = new File(listFiles.get(i));
	             FileReader fic = new FileReader(f);
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

