package fr.telecompt.shavadoop.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
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

import fr.telecompt.shavadoop.master.thread.ReceiveSlaveInfo;
import fr.telecompt.shavadoop.master.thread.ShufflingMapThread;
import fr.telecompt.shavadoop.master.thread.SplitMappingThread;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.LocalRepoFile;
import fr.telecompt.shavadoop.util.Nfs;
import fr.telecompt.shavadoop.util.PropertiesReader;

/**
 * Master object
 *
 */
public class Master extends Slave
{
	
	private final int WAITING_TIMES_SYNCHRO_THREAD = 10;
	
	// Map file and host
	private Map<String, String> filesHostMappers = new HashMap<String, String>();
	// Host who have a reduce file to assemble
	private List<String> hostReducers = new ArrayList<String>();
	
	public void initialize(){
		//TODO
//		try {
//			prop.setPropValue(PropertiesReader.MASTER_HOST, InetAddress.getLocalHost().getHostName());
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
	
	/**
	 * Launch MapReduce process
	 */
	public void launchMapReduce() {
		
		// get values from properties file
		String dsaFile = null;
		String fileIpAdress = null;
		String fileToTreat = null;
		try {
			dsaFile = prop.getPropValues(PropertiesReader.DSA_FILE);
			fileIpAdress = prop.getPropValues(PropertiesReader.IP_ADRESS_FILE);
			fileToTreat = prop.getPropValues(PropertiesReader.INPUT_FILE);
		} catch (IOException e) {e.printStackTrace();}
		
    	//Get our hostname mappers
		List<String> hostMappers = getHostMappers(fileIpAdress);
		
//		System.out.println(">>> " + hostMappers); 
		
    	//Get dsa key
		String dsaKey = getDsaKey(dsaFile);
		
//		System.out.println(">>> " + dsaKey);
    	
        // Split the file
    	List<String> filesToMap = inputSplitting(hostMappers, fileToTreat);
    	
//    	System.out.println(">>> " + filesToMap);
    	
        // Launch maps process
        Map<String, ArrayList<String>> groupedDictionary = manageMapThread(hostMappers, dsaKey, fileIpAdress, filesToMap);
        
        System.out.println(">>> " + groupedDictionary);
        
//        // Launch shuffling maps process
//        manageShufflingMapThread(groupedDictionary, dsaKey);
//        // Assembling final maps
//        assemblingFinalMaps();
	}
	
    /**
     * Split the original file
     * @param originalFile
     */
    public List<String> inputSplitting(List<String> hostMappers, String originalFile) {
    	System.out.println("Shavadoop workflow on : " + originalFile);
    	List<String> filesToMap = new ArrayList<String>();
		 try {
             FileReader fic = new FileReader(originalFile);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             int nbLine = 0;
             int nbFile = 0;
             
             // get the number of line of the file
             LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(originalFile)));
             lnr.skip(Long.MAX_VALUE);
             int totalLine = (lnr.getLineNumber() + 1);
             lnr.close();
             
             // Calculate the number of lines for each host
             int nbLineByHost = totalLine / (hostMappers.size() - 1);
             // The rest of the division for the last host
             int restLineByHost = totalLine - (nbLineByHost * hostMappers.size() - 1);
            
//             System.out.println(">>>nbLineByHost " + (nbLineByHost));
//             System.out.println(">>>restLineByHost " + (restLineByHost));
             
             // Content of the file
             List<String> content = new ArrayList<String>();
             
             while ((line = read.readLine()) != null) {
            	 // Add line by line to the content file
            	 content.add(line);
            	 ++nbLine;
            	 
            	// Write the complete file by block or if it's the end of the file
            	 if (nbLine == nbLineByHost
            			 || (nbLine == restLineByHost && nbFile == hostMappers.size() - 1)) {
                	 //For each group of line, we write a new file
                	 ++nbFile;
                	 String fileToMap = Constant.F_SPLITING + nbFile;
                	 Nfs.postFileToNFS(fileToMap, content);
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
		 System.out.println("Input splitting step done");
		 
		 return filesToMap;
    }
    

    /**
     * Launch a thread to execute map on each distant computer
     * @param dsaKey
     * @param fileIpAdress
     * @return grouped dictionary
     */
    public Map<String, ArrayList<String>> manageMapThread(List<String> hostMappers, String dsaKey, String fileIpAdress, List<String> filesToMap) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		//For each files to map
    	for (int i = 0; i < filesToMap.size(); i++) {
    		//We launch a map Thread to execute the map process on the distant computer
			es.execute(new SplitMappingThread(dsaKey, hostMappers.get(i), filesToMap.get(i)));
       	 	//We save the name of the file and the mapper
        	filesHostMappers.put(filesToMap.get(i), hostMappers.get(i));
    	}
    	es.shutdown();
    	
        // Create dictionary
    	Map<String, ArrayList<String>> groupedDictionary = null;
		try {
			groupedDictionary = groupedDictionary(createDictionary(hostMappers));
		} catch (IOException e) {e.printStackTrace();}
		
		
		
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("Parallel maps step done");

		return groupedDictionary;
    }
    
    
    /**
     * Create a dictionary with key (word) and value (file name)
     * @return
     * @throws IOException
     */
    public Map<String, String> createDictionary(List<String> hostMappers) throws IOException {
    	Map<String, String> dictionary = new HashMap<String, String>();
    	
		int port = 0;
		try {
			port = Integer.parseInt(prop.getPropValues(PropertiesReader.MASTER_PORT));
		} catch (IOException e) {e.printStackTrace();}
		
    	// Create dictionnary with socket
    	ServerSocket ss = new ServerSocket(port);
    	
    	// Threat to listen slaves info
    	ExecutorService es = Executors.newCachedThreadPool();

    	// While we haven't received all elements dictionary from the mappers
    	for (int i = 0; i < hostMappers.size(); i++) {
    		es.execute(new ReceiveSlaveInfo(ss, dictionary));
    		es.execute(new ShufflingMapThread(null, null, null, null));
    	}
    	
    	ss.close();
    	es.shutdown();
    	
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Dictionary done");
		
        return dictionary;
    }
    
    
    /**
     * Group each values by key
     * @param dictionary
     * @return grouped dictionary
     */
    private Map<String, ArrayList<String>> groupedDictionary(Map<String, String> dictionary) {
    	Map<String, ArrayList<String>> groupedDictionary = new HashMap<String, ArrayList<String>>();
    	
    	// Group by key
    	for (Entry<String, String> e : dictionary.entrySet()) {
    		String key_1 = e.getKey();
    		if (!groupedDictionary.containsKey(key_1)) {
	        	for (Entry<String, String> f : dictionary.entrySet()) {
	        		String key_2 = f.getKey();
	        		if (key_2.equalsIgnoreCase(key_1)) {
	        			// Get the list of files for this key
	        			ArrayList<String> listFiles = groupedDictionary.get(key_2);
	        			// Add this file for this key
	        			listFiles.add(f.getValue());
		        		groupedDictionary.put(key_2, listFiles);
	        		}
	        	}
    		}
    	}
    	return groupedDictionary;
    }
    
    
    /**
     * Launch a thread to execute shuffling map on each distant computer
     * @param dictionary
     */
    public void manageShufflingMapThread(Map<String, ArrayList<String>> dictionary, String dsaKey) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		
		
		//For each files to shuffling maps
		for (Entry<String, ArrayList<String>> e : dictionary.entrySet()) {
			// Get the list of files which refers to a same word
			List<String> listFiles = e.getValue();

		    // random choice of file's owner which contain the keyword
			int max = listFiles.size()-1;
			int min = 0;
		    int rd = new Random().nextInt((max - min) + 1) + min;
		    // Select the first host who has already one of files to do the shuffling map
			String hostOwner = filesHostMappers.get(listFiles.get(rd));
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
		    
			es.execute(new ShufflingMapThread(dsaKey, hostOwner, filesString, e.getKey()));
			hostReducers.add(hostOwner);
		}

		es.shutdown();
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Parallel Shuffling maps step done");
    }
    
    
    /**
     * Concat final maps together in one file result
     */
    public void assemblingFinalMaps() {
    	// Final file to reduce
    	String fileFinalResult = Constant.F_FINAL_RESULT;
    	// Get the list of file
    	String[] listFiles = null;
    	for (String host : hostReducers) {
    		//TODO search files on each distant computer with
    	}

    	// Concat data of each files in one
		try {
             Map<String, Integer> finalResult = new HashMap<String, Integer>();
             
             // For each files
			 for (int i = 0; i < listFiles.length; i++) {
	             FileReader fic = new FileReader(listFiles[i]);
	             BufferedReader read = new BufferedReader(fic);
	             String line = null;
	
	             // For each lines of the file
	             while ((line = read.readLine()) != null) {
		            String words[] = line.split(Constant.FILE_SEPARATOR);
		            // Add each line to our hashmap
		            finalResult.put(words[0], Integer.parseInt(words[1]));
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
    public List<String> getHostMappers(String fileIpAdress) {
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
