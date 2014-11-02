package fr.telecompt.shavadoop.master;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import fr.telecompt.shavadoop.master.thread.MapThread;
import fr.telecompt.shavadoop.master.thread.ShufflingMapThread;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.LocalRepoFile;
import fr.telecompt.shavadoop.util.Nfs;

/**
 * 
 *
 */
public class Master extends Slave
{
	private List<String> filesToMap = new ArrayList<String>();
	private Map<String, String> hostMappers = new HashMap<String, String>();
	private List<String> hostnameMappers;
	private String dsaKey;
	private final int WAITING_TIMES_SYNCHRO_THREAD = 10;
	
	public Master(String dsaFile, String fileIpAdress, String fileToTreat) {
		
    	//Get our hostname mappers
		hostnameMappers = getHostMappers(fileIpAdress, filesToMap.size());
    	//Get dsa key
    	dsaKey = getDsaKey(dsaFile);
    	
        // Split the file
        inputSplitting(fileToTreat);
        // Launch maps process
        manageMapThread(dsaFile, fileIpAdress);
        // Create dictionary
        Map<String, ArrayList<String>> groupedDictionary = createGroupedDictionary();
        // Launch shuffling maps process
        manageShufflingMapThread(groupedDictionary);
        // Assembling final maps
        assemblingFinalMaps();
	}
    
	public String getDsaKey(String dsaFile) {
		String dsaKey="";	

		try {
			InputStream ips=new FileInputStream(dsaFile); 
			InputStreamReader ipsr=new InputStreamReader(ips);
			BufferedReader br=new BufferedReader(ipsr);
			String line;
			while((line=br.readLine())!=null){
				dsaKey+=line+"\n";
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return dsaKey;
	}
    /**
     * Split the original file
     * @param originalFile
     */
    public void inputSplitting(String originalFile) {
    	System.out.println("Shavadoop workflow on : " + originalFile);
		 try {
             FileReader fic = new FileReader(originalFile);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             int nbFile = 1;
             
             while ((line = read.readLine()) != null) {
            	 //For each line, we write a new file
            	 String fileToMap = "S" + nbFile;
            	 Nfs.postFileToNFS(fileToMap, line);
            	 //We save names of theses files in a list
            	 filesToMap.add(fileToMap);
            	 ++nbFile;
             }
             
             fic.close();
             read.close();   
         } catch (IOException e) {
             e.printStackTrace();
         }
		 System.out.println("Input splitting step done");
    }

    /**
     * Launch a thread to execute map on each distant computer
     * @param fileIpAdress
     */
    public void manageMapThread(String dsaFile, String fileIpAdress) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		//For each files to map
    	for (int i = 0; i < filesToMap.size(); i++) {
    		//We launch a map Thread to execute the map process on the distant computer
			es.execute(new MapThread(dsaKey, hostnameMappers.get(i), filesToMap.get(i)));
       	 	//We save the name of the file and the mapper
        	hostMappers.put(filesToMap.get(i), hostnameMappers.get(i));
    	}
		es.shutdown();
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(WAITING_TIMES_SYNCHRO_THREAD, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Parallel maps step done");

    }
    
    /**
     * Return a hostname to do a map
     * @param fileIpAdress
     * @param nbMax
     * @return
     */
    public List<String> getHostMappers(String fileIpAdress, int nbMax) {
    	List<String> hostnameMappers = new ArrayList<String>();

		 try {
             FileReader fic = new FileReader(fileIpAdress);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             int nbHost = 0;
             
             while ((line = read.readLine()) != null
            		 && nbHost < nbMax) {
            	 hostnameMappers.add(line);
            	 ++nbHost;
             }
             fic.close();
             read.close();   
             
             //TODO optimize and combine function in slave project
    		 if (nbHost != nbMax) {
    			 System.out.println("Not enought host computers to execute the WordCount job");
    			 System.exit(1);
    		 }
    		 
         } catch (IOException e) {
             e.printStackTrace();
         }
		 
    	return hostnameMappers;
    }
    
    public Map<String, ArrayList<String>> createGroupedDictionary() {
    	Map<String, String> dictionary = new HashMap<String, String>();
    	// Create dictionnary with socket
    	//TODO
    	
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
    
    public void manageShufflingMapThread(Map<String, ArrayList<String>> dictionary) {
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		//For each files to shuffling maps
		for (Entry<String, ArrayList<String>> e : dictionary.entrySet()) {
			// Get the list of files which refers to a same word
			List<String> listFiles = e.getValue();
			// Select the first host who has already one of files to do the shuffling map
			String hostOwner = hostMappers.get(listFiles.get(0));
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
    
    public void assemblingFinalMaps() {
    	// Final file to reduce
    	String fileFinalResult = null;
    	// Get the list of file
    	String[] listFiles = null; //TODO search files from NFS
 
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
			 
        	 String fileToAssemble = "Final_result";
        	 LocalRepoFile.writeFile(fileToAssemble, finalResult);
        	 
         } catch (IOException e) {	
             e.printStackTrace();
         }
    }
}
