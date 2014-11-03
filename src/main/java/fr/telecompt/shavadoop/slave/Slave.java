package fr.telecompt.shavadoop.slave;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.LocalRepoFile;
import fr.telecompt.shavadoop.util.PropertiesReader;

/**
 * Slave object
 *
 */
public class Slave 
{
	
	public final static String SPLIT_MAPPING_FUNCTION = "split_mapping_function";
	public final static String SHUFFLING_MAP_FUNCTION = "shuffling_map_function";
	protected PropertiesReader prop = new PropertiesReader();
	
	
	public Slave(){}
	
	
    public Slave(String functionName, String fileToTreat, String key) {
    	switch (functionName){
    	case SPLIT_MAPPING_FUNCTION:
    		//Launch map method
    		splitMapping(fileToTreat);
    		break;
    	case SHUFFLING_MAP_FUNCTION:
    		//Lanch shuffling map method
    		String fileSortedMaps = shufflingMaps(key, fileToTreat);
    		//Launch reduce method	
    		mappingSortedMaps(fileSortedMaps);
    		break;
    	default:
    		System.out.println("Function name unknown");
    		break;
    	}
    }
    
    
    /**	
     * Map method
     * @param fileToMap
     */
    public void splitMapping(String fileToMap) {
		 try {
             FileReader fic = new FileReader(fileToMap);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             
             Map<String, Integer> unsortedMaps = null;
            		 
             while ((line = read.readLine()) != null) {
            	 unsortedMaps = wordCount(unsortedMaps, line);
             }
             
             fic.close();
             read.close();   
    		 
             // Write UM File
        	 String fileToShuffle = Constant.F_MAPPING;
        	 LocalRepoFile.writeFile(fileToShuffle, unsortedMaps);
        	 // Send dictionary with UNIQUE key (word) and hostname to the master
        	 sendDictionaryElement(unsortedMaps, fileToShuffle);
        	 
         } catch (IOException e) {
             e.printStackTrace();
         }
    }
    
    
    /**
     * Send key (word) - value (name of the file content), to the master
     * @param unsortedMaps
     * @param fileToShuffle
     * @throws IOException 
     * @throws UnknownHostException 
     */
    private void sendDictionaryElement(Map<String, Integer> unsortedMaps, String fileToShuffle) throws UnknownHostException, IOException {
    	//Get host master and port
		int port_master = 0;
		String host_master = null;
		try {
			port_master = Integer.parseInt(prop.getPropValues(PropertiesReader.MASTER_PORT));
			host_master = prop.getPropValues(PropertiesReader.MASTER_HOST);
		} catch (IOException e) {e.printStackTrace();}
		
        Socket socket = new Socket(host_master, port_master);

        BufferedReader plec = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter pred = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())),true);

        for (Entry<String, Integer> e : unsortedMaps.entrySet()) {
        	// Send dictionary element
        	pred.println(e.getKey() + Constant.SOCKET_SEPARATOR_MESSAGE + fileToShuffle);
        }

        // Send end message
        pred.println(Constant.SOCKET_END_MESSAGE
        		+ Constant.SOCKET_SEPARATOR_MESSAGE
        		+ "TODO"); //TODO Get the hostname / ip adress of this computer
        
        plec.close();
        pred.close();
        socket.close();
    }
    
    
    /**
     * Count the occurence of each word in the sentence
     * @param line
     * @return
     */
    public Map<String, Integer> wordCount(Map<String, Integer> mapWc, String line) {
    	if (mapWc == null) {
    		mapWc = new HashMap<String, Integer>();
    	}
    	//We split the line word by word
    	String words[] = line.split(Constant.SEPARATOR);
    	
    	for (int i = 0; i < words.length; i++) {
    		String word = words[i];
    		//Increment counter value for this word
    		if (mapWc.containsKey(word)) {
    			mapWc.put(word, mapWc.get(word) + 1);
    		} else {
    			mapWc.put(word, 1);
    		}
    	}
    	
    	return mapWc;
    }
    
    
    /**
     * Reduce method
     * @param fileToReduce
     */
    public void mappingSortedMaps(String fileToReduce) {
		 try {
             FileReader fic = new FileReader(fileToReduce);
             BufferedReader read = new BufferedReader(fic);
             String line = null;

             Map<String, Integer> finalMaps = new HashMap<String, Integer>();
             
             while ((line = read.readLine()) != null) {
            	String words[] = line.split(Constant.FILE_SEPARATOR);
         		//Increment counter value for this word
         		if (finalMaps.containsKey(words[0])) {
         			finalMaps.put(words[0], finalMaps.get(words[0]) + Integer.parseInt(words[1]));
         		} else {
         			finalMaps.put(words[0], Integer.parseInt(words[1]));
         		}
             }
             
        	 String fileToAssemble = Constant.F_REDUCING;
        	 LocalRepoFile.writeFile(fileToAssemble, finalMaps);
        	 
             fic.close();
             read.close();   
    		 
         } catch (IOException e) {
             e.printStackTrace();
         }
    }
    

    /**
     * Group and sort maps results by key
     * @param key
     * @param filesToTreat
     * @return
     */
    public String shufflingMaps(String key, String filesToTreat) {
    	// Final file to reduce
    	String fileToReduce = null;
    	// Get the list of file
    	String[] listFiles = filesToTreat.split(Constant.FILES_SHUFFLING_MAP_SEPARATOR);
    	//TODO search files from different computer
    	
    	// Concat data of each files in one
		 try {
             Map<String, Integer> sortedMaps = new HashMap<String, Integer>();
             
             // For each files
			 for (int i = 0; i < listFiles.length; i++) {
	             FileReader fic = new FileReader(listFiles[i]);
	             BufferedReader read = new BufferedReader(fic);
	             String line = null;
	
	             // For each lines of the file
	             while ((line = read.readLine()) != null) {
		            String words[] = line.split(Constant.FILE_SEPARATOR);
		            // Search line refers to this key
		            if (words[0].equalsIgnoreCase(key)) {
			            // Add each line matched with the key to our hashmap
		            	sortedMaps.put(words[0], Integer.parseInt(words[1]));
		            }
	             } 
	        	 
	             fic.close();
	             read.close();   
			 }
			 
        	 String fileToAssemble = Constant.F_SHUFFLING;
        	 LocalRepoFile.writeFile(fileToAssemble, sortedMaps);
        	 
         } catch (IOException e) {
             e.printStackTrace();
         }
    	return fileToReduce;
    	
    }
}
