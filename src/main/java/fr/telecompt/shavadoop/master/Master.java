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
import fr.telecompt.shavadoop.master.thread.ShufflingMapThread;
import fr.telecompt.shavadoop.master.thread.SplitMappingThread;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.LocalRepoFile;
import fr.telecompt.shavadoop.util.PropertiesReader;

/**
 * Master object
 *
 */
public class Master extends Slave
{
	private final Logger logger = LoggerFactory.getLogger(Master.class);
	private final int WAITING_TIMES_SYNCHRO_THREAD = 1;
	
	private String dsaKey = null;
	private List<String> hostsNetwork;
	private String fileIpAdress = null;
	private String usernameMaster = null;
	private int shellPort = 0;
	private int nbWorker = 0;
	private String dsaFile = null;
	private String fileToTreat = null;
	private String hostMaster = null;
	
	
	// Map file and host
	private Map<String, String> filesHostMappers = new HashMap<String, String>();
	
	public void initialize(){
		
		// create directory if not exist
		LocalRepoFile.createDirectory(new File(Constant.APP_PATH_REPO_RES));
		LocalRepoFile.createDirectory(new File(Constant.APP_PATH_REPO_LOG));

		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Initialize and clean " + Constant.APP_DEBUG_TITLE);
		
		usernameMaster = System.getProperty("user.name");

		// get values from properties file
		try {
			dsaFile = prop.getPropValues(PropertiesReader.FILE_DSA);
			fileIpAdress = prop.getPropValues(PropertiesReader.FILE_IP_ADRESS);
			fileToTreat = prop.getPropValues(PropertiesReader.FILE_INPUT);
			nbWorker = Integer.parseInt(prop.getPropValues(PropertiesReader.NB_WORKER));
			shellPort = Integer.parseInt(prop.getPropValues(PropertiesReader.PORT_SHELL));
		} catch (IOException e) {e.printStackTrace();}
		
		// get the list of hosts of the network
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Get hosts alive : " + Constant.APP_DEBUG_TITLE);
		hostsNetwork = getHostFromFile();
		
    	// get dsa key
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Get DSA Key : " + Constant.APP_DEBUG_TITLE);
		dsaKey = getDsaKey(dsaFile);
		if (Constant.APP_DEBUG) System.out.println("DSA Key : " + dsaKey);

		try {
			hostMaster = InetAddress.getLocalHost().getHostName().toString();
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
	public List<String> getHostAlive(int nbWorker) {
		List<String> hostAlive = new ArrayList<String>();
		
		for (String host : hostsNetwork) {
			if (hostAlive.size() < nbWorker) {
				if (isAlive(host)) {
					System.out.println("Worker : " + host);
					hostAlive.add(host);
				}
			} else {
				break;
			}
		}
		
		// if zero worker
		if (hostAlive.size() == 0) {
			// the master is the worker
			hostAlive.add(usernameMaster);
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
			Shell shell = new SSH(host, shellPort, usernameMaster, dsaKey);
			new Shell.Plain(shell).exec("echo " + host); 
			alive = true;
		} catch (Exception e) {} //fail to connect
		return alive;
	}
	
	/**
	 * Launch MapReduce process
	 */
	public void launchMapReduce() {
    	
        // Split the file
		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Input splitting : " + Constant.APP_DEBUG_TITLE);
    	List<String> filesToMap = inputSplitting(fileToTreat);
    	if (Constant.APP_DEBUG) System.out.println("Files after spliting : " + filesToMap);
    	
        // Launch maps process
    	if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch map threads : " + Constant.APP_DEBUG_TITLE);
		List<String> hostMappers = getHostAlive(nbWorker);
		if (Constant.APP_DEBUG) System.out.println("Host mappers : " + hostMappers); 
        Map<String, ArrayList<String>> groupedDictionary = manageMapThread(hostMappers, filesToMap);
        if (Constant.APP_DEBUG) System.out.println("Grouped dictionary : " + groupedDictionary);
        
        // Launch shuffling maps process
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Launch shuffling threads : " + Constant.APP_DEBUG_TITLE);
        Map<String, String> filesHostReducers = manageShufflingMapThread(groupedDictionary);
        // Assembling final maps
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Assembling final maps : " + Constant.APP_DEBUG_TITLE);
        assemblingFinalMaps(filesHostReducers);
        
        if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " MapReduce process done " + Constant.APP_DEBUG_TITLE);
	}
	
    /**
     * Split the original file
     * @param originalFile
     */
    public List<String> inputSplitting(String fileToTreat) {
    	
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

	         // if it's a cluster
             if (nbWorker > 1) {
	             // Calculate the number of lines for each host
	             nbLineByHost = totalLine / (nbWorker - 1);
	             // The rest of the division for the last host
	             restLineByHost = totalLine - (nbLineByHost * nbWorker - 1);
             }
            
             if (Constant.APP_DEBUG) System.out.println("Nb host mappers : " + (nbWorker));
             if (Constant.APP_DEBUG) System.out.println("Nb line by host mapper : " + (nbLineByHost));
             if (Constant.APP_DEBUG) System.out.println("Nb line for the last host mapper : " + (restLineByHost));
             
             // Content of the file
             List<String> content = new ArrayList<String>();
             
             while ((line = read.readLine()) != null) {
            	 // Add line by line to the content file
            	 content.add(line);
            	 ++nbLine;
            	 
            	// Write the complete file by block or if it's the end of the file
            	 if (nbLine == nbLineByHost
            			 || (nbLine == restLineByHost && nbFile == nbWorker - 1)) {
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
     * Launch a thread to execute map on each distant computer
     * @param dsaKey
     * @param fileIpAdress
     * @return grouped dictionary
     */
    public Map<String, ArrayList<String>> manageMapThread(List<String> hostMappers, List<String> filesToMap) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		//For each files to map
    	for (int i = 0; i < filesToMap.size(); i++) {
    		//We launch a map Thread to execute the map process on the distant computer
			es.execute(new SplitMappingThread(usernameMaster, dsaKey, hostMappers.get(i), filesToMap.get(i), hostMaster));
       	 	//We save the name of the file and the mapper
        	filesHostMappers.put(filesToMap.get(i), hostMappers.get(i));
    	}
    	es.shutdown();
    	
        // Create dictionary
    	Map<String, ArrayList<String>> groupedDictionary = null;
		try {
			groupedDictionary = createDictionary(hostMappers);
		} catch (IOException e) {e.printStackTrace();}
		
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
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
    public Map<String, ArrayList<String>> createDictionary(List<String> hostMappers) throws IOException {
    	
    	Map<String, ArrayList<String>> dictionary = new HashMap<String, ArrayList<String>>();
    	
		int port = 0;
		try {
			port = Integer.parseInt(prop.getPropValues(PropertiesReader.PORT_MASTER));
		} catch (IOException e) {e.printStackTrace();}
		
    	// Create dictionnary with socket
    	ServerSocket ss = new ServerSocket(port);
    	
    	// Threat to listen slaves info
    	ExecutorService es = Executors.newCachedThreadPool();

    	// While we haven't received all elements dictionary from the mappers
    	for (int i = 0; i < hostMappers.size(); i++) {
    		es.execute(new ReceiveSlaveInfo(ss, dictionary));
    	}
    	
    	es.shutdown();
    	
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
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
    public Map<String, String> manageShufflingMapThread(Map<String, ArrayList<String>> dictionary) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		// Host who have a reduce file to assemble
		Map<String, String> filesHostReducers = new HashMap<String, String>();
		
		//For each files to shuffling maps
		for (Entry<String, ArrayList<String>> e : dictionary.entrySet()) {
			// Get the list of files which refers to a same word
			ArrayList<String> listFiles = e.getValue();

		    // random choice of file's owner which contain the keyword
			int max = listFiles.size()-1;
			int min = 0;
		    int rd = new Random().nextInt((max - min) + 1) + min;

		    // Select the first host who has already one of files to do the shuffling map
			String hostOwner = listFiles.get(rd).split(Constant.F_SEPARATOR)[1];

			// the master is not the worker
			if (nbWorker > 0) {
				// if this host is not alive
				if (isAlive(hostOwner)) {
					// we select an other
					hostOwner = getHostAlive(1).get(0);
				}
			}
			
			// Parse the list of file to build a String with urls
			String filesString = "";
			for (String file : listFiles) {
				filesString += file + Constant.FILES_SHUFFLING_MAP_SEPARATOR;
			}
			// Remove the last separator
		    if (filesString.length() > 0
		    		&& Character.toString(filesString.charAt(filesString.length()-1)).equals(Constant.FILES_SHUFFLING_MAP_SEPARATOR)) {
		    	filesString = filesString.substring(0, filesString.length()-1);
		    }
		    
		    if (Constant.APP_DEBUG) System.out.println("Launch shuffling map thread for the key : " + e.getKey() + " on " + hostOwner + " (" + filesString + ")");
		    
			es.execute(new ShufflingMapThread(usernameMaster, dsaKey, hostOwner, filesString, e.getKey(), hostMaster));
			filesHostReducers.put(e.getKey(), hostOwner);
		}

		es.shutdown();
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
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
			e.printStackTrace();
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
