package fr.telecompt.shavadoop.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.master.thread.ReceiveSlaveInfo;
import fr.telecompt.shavadoop.master.thread.LaunchShufflingMap;
import fr.telecompt.shavadoop.master.thread.LaunchSplitMapping;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.LocalRepoFile;
import fr.telecompt.shavadoop.util.PropertiesReader;
import fr.telecompt.shavadoop.util.Util;

/**
 * Master object
 *
 */
public class Master extends Slave
{
	
	private final Logger logger = LoggerFactory.getLogger(Master.class);
	
	private String dsaKey = null;
	private List<String> hostsNetwork;
	private String fileIpAdress = null;
	private int shellPort = 0;
	private String dsaFile = null;
	private String fileToTreat = null;
	private String hostMaster = null;
	private String hostMasterFull = null;
	private int nbWorkerMax = 1;
	// Map file and host
	private Map<String, String> filesHostMappers = new HashMap<String, String>();
	private int workersMapper;
	private double speedUp = 1; // loi de adam //TODO
	private int threadByCore;
	
	/**
	 * Clean and initialize the MapReduce process
	 */
	public void initialize(){
		
		// create directory if not exist
		LocalRepoFile.createDirectory(new File(Constant.APP_PATH_REPO_RES));
		LocalRepoFile.createDirectory(new File(Constant.APP_PATH_REPO_LOG));

		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Initialize and clean " + Constant.APP_DEBUG_TITLE);

		// get values from properties file
		dsaFile = prop.getPropValues(PropertiesReader.FILE_DSA);
		fileIpAdress = prop.getPropValues(PropertiesReader.FILE_IP_ADRESS);
		fileToTreat = prop.getPropValues(PropertiesReader.FILE_INPUT);
		nbWorkerMax = Integer.parseInt(prop.getPropValues(PropertiesReader.WORKER));
		shellPort = Integer.parseInt(prop.getPropValues(PropertiesReader.PORT_SHELL));
		threadByCore = Integer.parseInt(prop.getPropValues(PropertiesReader.THREAD_BY_CORE));
		
		// get the list of hosts of the network
		hostsNetwork = getHostFromFile();
		
    	// get dsa key
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Get DSA Key : " + Constant.APP_DEBUG_TITLE);
		dsaKey = getDsaKey(dsaFile);
		if (Constant.APP_DEBUG) System.out.println(dsaKey);

		try {
			hostMaster = InetAddress.getLocalHost().getHostName();
			hostMasterFull = InetAddress.getLocalHost().getCanonicalHostName();
			Constant.USERNAME_MASTER = System.getProperty("user.name");
		} catch (Exception e) {e.printStackTrace();}
		
		// clean directory
		LocalRepoFile.cleanDirectory(new File(Constant.APP_PATH_REPO_RES)); 
		LocalRepoFile.cleanDirectory(new File(Constant.APP_PATH_REPO_LOG));
	}
	
	/**
	 * Return x hosts alive
	 * @param hosts
	 * @param nbHost
	 * @param shellPort
	 * @param usernameMaster
	 * @param dsaKey
	 * @return
	 */
	public List<String> getHostAliveCores(int nbWorker) {
		List<String> hostAlive = new ArrayList<String>();
		
		// check first for this computer : the master is the worker
		for (int i = 0; i < cores; i++) {
			// the number of worker is the number of cores of this computer
			if (hostAlive.size() < nbWorker) {
				hostAlive.add(hostMasterFull);
			} else {
				break;
			}
		}
		
		// if need more worker, use the distant computer
		if (hostAlive.size() < nbWorker) {
			for (String host : hostsNetwork) {
				if (hostAlive.size() < nbWorker) {
					if (isAlive(host)) {
						for (int i = 0; i < getCoresNumber(host); i++) {
							if (hostAlive.size() < nbWorker) {
								hostAlive.add(host);
							} else {
								break;
							}
						}
					}
				} else {
					break;
				}
			}
		}
		
		return hostAlive;
	}
	
	/**
	 * Test if a host is alive
	 * @param host
	 * @param shellPort
	 * @param usernameMaster
	 * @param dsaKey
	 * @return true if it's alive
	 */
	public boolean isAlive(String host) {
		boolean alive = false;
		// test if this host is alive
		try {
			//Connect to the distant computer
			Shell shell = new SSH(host, shellPort, Constant.USERNAME_MASTER, dsaKey);
			new Shell.Plain(shell).exec("echo " + host); 
			alive = true;
		} catch (Exception e) {} //fail to connect
		return alive;
	}
	
	/**
	 * Return the cores number from the distant computer
	 * @param host
	 * @return cores
	 */
	public int getCoresNumber(String host) {
		int cores = 1;
		// test if this host is alive
		try {
			// connect to the distant computer
			Shell shell = new SSH(host, shellPort, Constant.USERNAME_MASTER, dsaKey);
			// get the number of cores
			String cmd = "grep -c ^processor /proc/cpuinfo";
			String stdout = new Shell.Plain(shell).exec(cmd);
			cores = Integer.parseInt(stdout.trim()); 
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cores;
	}
	
	/**
	 * Launch MapReduce process
	 */
	public void launchMapReduce() {
    	
		// get workers
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Get workers core alive : " + Constant.APP_DEBUG_TITLE);
		List<String> workersMapperCores = getHostAliveCores(nbWorkerMax);
		if (Constant.APP_DEBUG) System.out.println("Workers core : " + workersMapperCores); 
		
        // Split the file
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Input splitting : " + Constant.APP_DEBUG_TITLE);
		List<String> filesToMap = inputSplitting(workersMapperCores, fileToTreat);
    	if (Constant.APP_DEBUG) System.out.println("Files after spliting : " + filesToMap);
    	
        // Launch maps process
    	if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch map threads : " + Constant.APP_DEBUG_TITLE);
        Map<String, ArrayList<String>> groupedDictionary = launchMapThreads(workersMapperCores, filesToMap);
        if (Constant.APP_DEBUG) System.out.println("Grouped dictionary : " + groupedDictionary);
        
        // Launch shuffling maps process
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch shuffling threads : " + Constant.APP_DEBUG_TITLE);
        Map<String, String> filesHostReducers = launchShufflingMapThreads(groupedDictionary);
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
             int nbLine = 0;
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

             
             if (Constant.APP_DEBUG) System.out.println("Nb workers mappers : " + (workersMapper));
             if (Constant.APP_DEBUG) System.out.println("Nb line to tread : " + (totalLine));
             if (Constant.APP_DEBUG) System.out.println("Nb line by host mapper : " + (nbLineByHost));
             if (Constant.APP_DEBUG) System.out.println("Nb line for the last host mapper : " + (restLineByHost));
             
             // Content of the file
             List<String> content = new ArrayList<String>();
             
             while ((line = read.readLine()) != null) {
            	 line = cleanText(line);
            	 // Add line by line to the content file
            	 content.add(line);
            	 ++nbLine;
            	 // Write the complete file by block or if it's the end of the file
            	 if ((nbLine == nbLineByHost && nbFile < workersMapper - 1)
            			 || (nbLine == nbLineByHost + restLineByHost && nbFile == workersMapper - 1)) {
                	 //For each group of line, we write a new file
                	 ++nbFile;
                	 String fileToMap = Constant.F_SPLITING + nbFile;
                	 LocalRepoFile.writeFile(fileToMap, content);
                	 //We save names of theses files in a list
                	 filesToMap.add(fileToMap);
                	 // Reset
                	 nbLine = 0;
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
     * Clean the line
     * @param line
     * @return line clean
     */
    public String cleanText(String line) {
    	String clean = line;
    	// clean the non alpha numeric character or space
    	clean = clean.replaceAll("[^a-zA-Z0-9\\s]", "");
    	// just one space beetween each words
    	clean = clean.replaceAll("\\s+", " ");
    	return clean;
    }

    /**
     * Launch a thread to execute map on each distant computer
     * @param dsaKey
     * @param fileIpAdress
     * @return grouped dictionary
     */
    public Map<String, ArrayList<String>> launchMapThreads(List<String> workersMapperCores, List<String> filesToMap) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		if (Constant.APP_DEBUG) System.out.println("Nb workers mappers : " + workersMapper);
		if (Constant.APP_DEBUG) System.out.println("Nb files splitted : " + filesToMap.size());
		//For each files to map
    	for (int i = 0; i < filesToMap.size(); i++) {
    		//We launch a map Thread to execute the map process on the distant computer
			es.execute(new LaunchSplitMapping(dsaKey, workersMapperCores.get(i), filesToMap.get(i), hostMaster));
       	 	//We save the name of the file and the mapper
        	filesHostMappers.put(filesToMap.get(i), workersMapperCores.get(i)); 
    	}
    	es.shutdown();
    	
        // Create dictionary
    	Map<String, ArrayList<String>> groupedDictionary = null;
		try {
			groupedDictionary = createDictionary(workersMapperCores);
		} catch (IOException e) {e.printStackTrace();}
		
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(Constant.WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
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
    public Map<String, ArrayList<String>> createDictionary(List<String> workers) throws IOException {
    	
    	Map<String, ArrayList<String>> dictionary = new HashMap<String, ArrayList<String>>();
    	
		int port = Integer.parseInt(prop.getPropValues(PropertiesReader.PORT_MASTER));
		
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
			es.awaitTermination(Constant.WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		ss.close();
		
        return dictionary;
    }
    
    
    /**
     * Launch a thread to execute shuffling map on each distant computer
     * @param dictionary
     */
    public Map<String, String> launchShufflingMapThreads(Map<String, ArrayList<String>> dictionary) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		// Host who have a reduce file to assemble
		Map<String, String> filesHostReducers = new HashMap<String, String>();
		
		// get a list of workers reducer alive
		List<String> workersReducerCores = getHostAliveCores(nbWorkerMax);
		
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
        
        if (Constant.APP_DEBUG) System.out.println("Nb workers core reducer : " + (workersReducer));
        if (Constant.APP_DEBUG) System.out.println("Nb reduce to do : " + (totalReducerTodo));
        if (Constant.APP_DEBUG) System.out.println("Nb reduce by core : " + (nbThreadByCore));
        if (Constant.APP_DEBUG) System.out.println("Nb reduce for the last core : " + (restThreadByCore));
        
		String listFilesString = "";
		List<String> listKey = new ArrayList<String>();
		// the number of light threads by computer
		int cptLightThread = 0;
		
		//For each key and files to shuffling maps
		for (Entry<String, ArrayList<String>> e : dictionary.entrySet()) {
			
			listFilesString += Util.listToString(e.getValue()) + Constant.FILES_BLOC_SHUFFLING_MAP_SEPARATOR;
			listKey.add(e.getKey());
			filesHostReducers.put(e.getKey(), workerReducer);
			++cptLightThread;
			
			if ((cptLightThread == nbThreadByCore && cptLightThread < workersReducer - 1)
					|| (cptLightThread == nbThreadByCore + restThreadByCore && cptLightThread == workersReducer - 1)) {
				// Remove the last separator
			    if (listFilesString.length() > 0 && Character.toString(listFilesString.charAt(listFilesString.length()-1)).equals(Constant.FILES_BLOC_SHUFFLING_MAP_SEPARATOR)) {
			    	listFilesString = listFilesString.substring(0, listFilesString.length()-1);
			    }
				// launch shuffling map process
				es.execute(new LaunchShufflingMap(dsaKey, workerReducer, listFilesString, Util.listToString(listKey), hostMaster));
				// if enought heavy threads launch for one distant computer, change the worker reducer
				++idWorkerReducerCore;
				workerReducer = workersReducerCores.get(idWorkerReducerCore);
				cores = getCoresNumber(workerReducer);
				// reset 
				listFilesString = "";
				listKey = new ArrayList<String>();
				cptLightThread = 0;
       	 	}
		}
		
		//TODO si pas asser de cores dispo, il faut gerer ça -> adapter le nombre de threads par core en fonction du nombre de reduce à faire
		//TODO nb thread max by core ?
		
		es.shutdown();
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(Constant.WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
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
    	System.out.println(">>> " + listFiles); //TODO
    	for (Entry<String, String> e : filesHostReducers.entrySet()) {
    		listFiles.add(Constant.F_REDUCING 
    				+ Constant.F_SEPARATOR 
    				+ e.getKey() // keyword
    				+ Constant.F_SEPARATOR 
    				+ e.getValue()); // hostname
    	}

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
			 
        	 LocalRepoFile.writeFile(fileFinalResult, finalResult);
        	 
         } catch (IOException e) {	
             e.printStackTrace();
         }
    }
    
    
    /**
     * Return the dsa key
     * @param dsaFile
     * @return dsa key
     */
	public String getDsaKey(String dsaFile) {
		String dsaKey = "";	

		try {
			InputStream ips=new FileInputStream(dsaFile); 
			InputStreamReader ipsr=new InputStreamReader(ips);
			BufferedReader br=new BufferedReader(ipsr);
			String line;
			while((line=br.readLine())!=null){
				dsaKey += line + "\n";
			}
			br.close();
		} catch (IOException e) {
			System.out.println("No dsa file");
		}

		return dsaKey;
	}
	
    /**
     * Return list of hostname from a file
     * @param fileIpAdress
     * @return
     */
    public List<String> getHostFromFile() {
    	List<String> hostnameMappers = new ArrayList<String>();

		 try {
             FileReader fic = new FileReader(fileIpAdress);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             
             while ((line = read.readLine()) != null) {
            	 hostnameMappers.add(line);
             }
             fic.close();
             read.close();   
             
         } catch (IOException e) {
             e.printStackTrace();
         }
		 
    	return hostnameMappers;
    }
}

