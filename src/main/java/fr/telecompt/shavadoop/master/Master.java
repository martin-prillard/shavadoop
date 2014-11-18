package fr.telecompt.shavadoop.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;

import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.tasktracker.TaskTracker;
import fr.telecompt.shavadoop.thread.FileTransfert;
import fr.telecompt.shavadoop.thread.LaunchShufflingMap;
import fr.telecompt.shavadoop.thread.LaunchSplitMapping;
import fr.telecompt.shavadoop.thread.DictionaryManager;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.Pair;
import fr.telecompt.shavadoop.util.PropReader;
import fr.telecompt.shavadoop.util.Util;

/**
 * Master object
 *
 */
public class Master
{
	private String fileInputCleaned = null;
	private int portMasterDictionary;
	private int portTaskTracker;
	private int nbWorkerMax;
	private int nbWorker;
	private PropReader prop = new PropReader();
	private SSHManager sm;
	private int nbWorkerMappers;
	private double startTime;
	
	// dictionary
	Map<String, HashSet<Pair>> dictionaryMapping; // worker, (host, UM_Wx file) -> to to shuffling
	Map<String, String> dictionaryReducing; // idWorker, host -> to get all RM files
	

	public Master(SSHManager _sm){
		startTime = System.currentTimeMillis();
		sm = _sm;
	}
	
	
	/**
	 * Clean and initialize the MapReduce process
	 */
	public void initialize(){
		
		if (Constant.MODE_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Initialize and clean " + Constant.APP_DEBUG_TITLE);
		
		// get values from properties file
		String fileToTreat = prop.getPropValues(PropReader.FILE_INPUT);
		nbWorkerMax = Integer.parseInt(prop.getPropValues(PropReader.WORKER_MAX));
    	portMasterDictionary = Integer.parseInt(prop.getPropValues(PropReader.PORT_MASTER_DICTIONARY));
    	portTaskTracker = Integer.parseInt(prop.getPropValues(PropReader.PORT_TASK_TRACKER));
		if (Constant.MODE_DEBUG) System.out.println("Variables initialized");
		
		// clean res directory
		Util.initializeResDirectory(Constant.PATH_REPO_RES);
		
		// clean the input text
		fileInputCleaned = Constant.PATH_F_INPUT_CLEANED;
		Util.cleanText(fileToTreat, fileInputCleaned);
		if (Constant.MODE_DEBUG) System.out.println("file " + fileInputCleaned + " cleaned");
		
	}
	
	
	/**
	 * Launch MapReduce process
	 */
	public void launchMapReduce() {
    	
		// get workers
		if (Constant.MODE_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Get workers core alive : " + Constant.APP_DEBUG_TITLE);
		List<String> workersCores = sm.getHostAliveCores(nbWorkerMax, false);
		nbWorker = workersCores.size();
		if (Constant.MODE_DEBUG) System.out.println("Workers core : " + workersCores); 
		
    	if (Constant.MODE_DEBUG) System.out.println("Shavadoop workflow on : " + fileInputCleaned);
    	
        // Split the file : master
		if (Constant.MODE_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Input splitting : " + Constant.APP_DEBUG_TITLE);
		List<String> filesToMap = inputSplitting(workersCores, fileInputCleaned);
    	
        // Launch maps process : master & slave
    	if (Constant.MODE_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch map threads : " + Constant.APP_DEBUG_TITLE);
        dictionaryMapping = launchSplitMappingThreads(workersCores, filesToMap);
        if (Constant.MODE_DEBUG) System.out.println("Mapping dictionary's size : " + dictionaryMapping.size());
        
        // Launch shuffling maps process : master & slave
        if (Constant.MODE_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch shuffling threads : " + Constant.APP_DEBUG_TITLE);
		try {
			dictionaryReducing = launchShufflingMapThreads(workersCores);
		} catch (IOException e) {e.printStackTrace();}
		
        // Assembling final maps : master
        if (Constant.MODE_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Assembling final maps : " + Constant.APP_DEBUG_TITLE);
        assemblingFinalMaps();
        
        double totalTime = (System.currentTimeMillis() - startTime);
        totalTime = (double) ((totalTime / (1000.0*60.0)) % 60.0);
        
        if (Constant.MODE_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " MapReduce process done in " + totalTime + " minutes " + Constant.APP_DEBUG_TITLE);
	}
	
	
    /**
     * Split the original file
     * @param originalFile
     */
    public List<String> inputSplitting(List<String> workers, String fileToTreat) {
    	
    	List<String> filesToMap;

    	nbWorkerMappers = workers.size();
    	
        if (Constant.MODE_DEBUG) System.out.println("Nb workers mappers : " + (nbWorkerMappers) + " " + workers);
        
         long sizeFileToTreat = new File(fileToTreat).length();
         int totalBloc;
         
         // split by line
         if (sizeFileToTreat < Constant.BLOC_SIZE_MIN) {
        	 totalBloc = Util.getFileNumberLine(fileToTreat);
         // split by bloc
         } else {
        	 totalBloc = (int) Math.ceil((double) sizeFileToTreat / (double) Constant.BLOC_SIZE_MIN);
         }
         
         // if too more worker available for map process
         if (nbWorkerMappers > totalBloc) {
        	 nbWorkerMappers = totalBloc;
         }
         // The rest of the division for the last host
         int restBlocByHost = totalBloc % nbWorkerMappers;
         // Calculate the number of lines for each host
         int nbBlocByHost = (totalBloc - restBlocByHost) / (nbWorkerMappers);
         
         // split by line
         if (sizeFileToTreat < Constant.BLOC_SIZE_MIN) {
             if (Constant.MODE_DEBUG) System.out.println("Nb line to tread : " + (totalBloc));
             if (Constant.MODE_DEBUG) System.out.println("Nb line by host mapper : " + (nbBlocByHost));
             if (Constant.MODE_DEBUG) System.out.println("Nb line for the last host mapper : " + (restBlocByHost));
             filesToMap =  Util.splitByLineFile(fileToTreat, nbBlocByHost, restBlocByHost, nbWorkerMappers);
         } else {
        	//split by bloc
             if (Constant.MODE_DEBUG) System.out.println("Nb bloc (" + Constant.BLOC_SIZE_MIN + " MB) to tread : " + (totalBloc));
             if (Constant.MODE_DEBUG) System.out.println("Nb bloc (" + Constant.BLOC_SIZE_MIN + " MB) by host mapper : " + (nbBlocByHost));
             if (Constant.MODE_DEBUG) System.out.println("Nb bloc (" + Constant.BLOC_SIZE_MIN + " MB) for the last host mapper : " + (restBlocByHost));
             filesToMap = Util.splitLargeFile(fileToTreat, nbBlocByHost, restBlocByHost, nbWorkerMappers);
         }
		 
		 return filesToMap;
    }
    

    /**
     * Launch a thread to execute map on each distant computer
     * @param dsaKey
     * @param fileIpAdress
     * @return grouped dictionary
     */
    public Map<String, HashSet<Pair>> launchSplitMappingThreads(List<String> workersMapperCores, List<String> filesToMap) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		TaskTracker ts = new TaskTracker(sm, es, portTaskTracker, String.valueOf(nbWorker), null);
		es.execute(ts);
		
		if (Constant.MODE_DEBUG) System.out.println("Nb workers mappers : " + workersMapperCores.size());
		if (Constant.MODE_DEBUG) System.out.println("Nb files splitted : " + filesToMap.size());
		
		// dictionary
    	Map<String, HashSet<Pair>> dictionaryMapping = new HashMap<String, HashSet<Pair>>();
    	// listener to get part dictionary from the worker mappers
    	es.execute(new DictionaryManager(portMasterDictionary, nbWorkerMappers, dictionaryMapping));
    	
    	int idWorkerMapperCore = 0;
    	
		//For each files to map
    	for (int i = 0; i < filesToMap.size(); i++) {
	    	try {
	    	    Thread.sleep(100); // down the speed, use to not interfer with the listener slave thread
	    	} catch(InterruptedException ex) {
	    	    Thread.currentThread().interrupt();
	    	}
	    	Thread smt = new LaunchSplitMapping(sm, String.valueOf(nbWorker), workersMapperCores.get(i), filesToMap.get(i), sm.isLocal(workersMapperCores.get(i)), sm.getHost(), Integer.toString(idWorkerMapperCore));
			es.execute(smt);
			ts.addTask(smt, workersMapperCores.get(i), Integer.toString(idWorkerMapperCore), Slave.SPLIT_MAPPING_FUNCTION, filesToMap.get(i), null);
			++idWorkerMapperCore;
    	}
		
    	if (Constant.MODE_DEBUG) System.out.println("Waitting the end of maps process...");
    	
    	
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(Constant.THREAD_MAX_LIFETIME, TimeUnit.MINUTES);
		} catch (InterruptedException e) {e.printStackTrace();}

		return dictionaryMapping;
    }
    
    
    /**
     * Launch a thread to execute shuffling map on each distant computer
     * @param dictionary
     * @throws IOException 
     */
    public Map<String, String> launchShufflingMapThreads(List<String> workersCores) throws IOException {
		// Host who have a reduce file to assemble
    	Map<String, String> dictionaryReducing = new HashMap<String, String>();
		
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		TaskTracker ts = new TaskTracker(sm, es, portTaskTracker, String.valueOf(nbWorker), dictionaryReducing);
		es.execute(ts);
		
		//For each key and files to shuffling maps
		for (Entry<String, HashSet<Pair>> e : dictionaryMapping.entrySet()) {
			
			int idWorkerReducerCore = Integer.valueOf(e.getKey());
			String workerReducer = workersCores.get(idWorkerReducerCore);
			
			// File output
			String shufflingDictionaryFile = 
					Constant.PATH_F_SHUFFLING_DICTIONARY 
					+ Constant.SEP_NAME_FILE 
					+ idWorkerReducerCore;
			FileWriter fw = new FileWriter(shufflingDictionaryFile);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter write = new PrintWriter(bw); 
			
			for (Pair p : e.getValue()) {
				write.println(p.getVal1()
			    		+ Constant.SEP_CONTAINS_FILE
			    		+ p.getVal2()); 
			}

			write.close();
			
			dictionaryReducing.put(Integer.toString(idWorkerReducerCore), workerReducer);
			
			// launch shuffling map process
			Thread smt = new LaunchShufflingMap(sm, String.valueOf(nbWorker), workerReducer, shufflingDictionaryFile, sm.getHost(), Integer.toString(idWorkerReducerCore));
			es.execute(smt);
			ts.addTask(smt, workerReducer, Integer.toString(idWorkerReducerCore), Slave.SHUFFLING_MAP_FUNCTION, shufflingDictionaryFile, e.getKey());
		}
		
		if (Constant.MODE_DEBUG) System.out.println("Waitting the end of shuffling maps process...");
		
		try {
			es.awaitTermination(Constant.THREAD_MAX_LIFETIME, TimeUnit.MINUTES);
		} catch (InterruptedException e) {e.printStackTrace();}
		
		return dictionaryReducing;
    }
    
    
    /**
     * Concat final maps together in one file result
     */
    public void assemblingFinalMaps() {
    	
    	// Final file to reduce
    	String fileFinalResult = Constant.PATH_F_FINAL_RESULT;
    	// Get the list of file
    	Set<String> listFiles = new HashSet<String>();
    	 
    	ExecutorService esModeScp = null;
    	if (Constant.MODE_SCP_FILES) {
    		esModeScp = Executors.newCachedThreadPool();
    	}
    	
    	for (Entry<String, String> e : dictionaryReducing.entrySet()) {
    		
    		String idWorker = e.getKey();
    		String worker = e.getValue();
    		String nameFileToMerge = Constant.PATH_F_REDUCING 
    				+ Constant.SEP_NAME_FILE 
    				+ worker // hostname
    				+ Constant.SEP_NAME_FILE 
           			+ idWorker; //id worker
    		
    		if (Constant.MODE_SCP_FILES) {
    			// if it's a slave's file and not a master's file
    			if (!worker.equalsIgnoreCase(sm.getHostFull())) {
		    		// MASTER <- SLAVE files
		    		String destFile = Constant.PATH_SLAVE + FilenameUtils.getBaseName(nameFileToMerge);
					// if the file doesn't exist on this computer
					File f = new File(destFile);
					if (!f.exists()) {
						esModeScp.execute(new FileTransfert(sm, worker, nameFileToMerge, destFile));
			    		nameFileToMerge = destFile;
					}
    			}
    		}
    		
    		listFiles.add(nameFileToMerge); 
    	}

    	if (Constant.MODE_SCP_FILES) {
	    	esModeScp.shutdown();
			try {
				esModeScp.awaitTermination(Constant.THREAD_MAX_LIFETIME, TimeUnit.MINUTES);
			} catch (InterruptedException e) {e.printStackTrace();}
    	}
		
    	if (Constant.MODE_DEBUG) System.out.println("Nb files to merge : " + listFiles.size());
    	if (Constant.MODE_DEBUG) System.out.println("Waitting the end of merging process...");
    	
    	// Concat data of each files in one
		try {
             Map<String, Integer> finalResult = new HashMap<String, Integer>();
             
             // For each files
             for (Iterator<String> it = listFiles.iterator(); it.hasNext(); ) {
            	 String file = it.next();
				 File f = new File(file);
	             FileReader fic = new FileReader(f);
	             BufferedReader read = new BufferedReader(fic);
	             String line = null;
	
	             // For each lines of the file
	             while ((line = read.readLine()) != null) {
		            String words[] = line.split(Constant.SEP_CONTAINS_FILE);
		            // Add each line to our hashmap
		            finalResult.put(words[0], Integer.parseInt(words[1]));
		            
		            if (Constant.MODE_DEBUG) System.out.println(words[0] + Constant.SEP_CONTAINS_FILE + Integer.parseInt(words[1]));
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

