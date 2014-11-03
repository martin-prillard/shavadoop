package fr.telecompt.shavadoop.slave;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.LocalRepoFile;
import fr.telecompt.shavadoop.util.Pair;
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
    		mappingSortedMaps(key, fileSortedMaps);
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
             
             List<Pair> unsortedMaps = null;
            		 
             while ((line = read.readLine()) != null) {
            	 unsortedMaps = wordCount(unsortedMaps, line);
             }
             
             fic.close();
             read.close();   
    		 
             // Write UM File
        	 String fileToShuffle = Constant.F_MAPPING + Constant.F_SEPARATOR + InetAddress.getLocalHost().getHostName();
        	 LocalRepoFile.writeFileFromPair(fileToShuffle, unsortedMaps);
        	 // Send dictionary with UNIQUE key (word) and hostname to the master
        	 sendDictionaryElement(unsortedMaps, fileToShuffle);
        	 
         } catch (IOException e) {
             e.printStackTrace();
         }
    }
    
    /**
     * Count the occurence of each word in the sentence
     * @param line
     * @return
     */
    public List<Pair> wordCount(List<Pair> mapWc, String line) {
    	if (mapWc == null) {
    		mapWc = new ArrayList<Pair>();
    	}
    	//We split the line word by word
    	String words[] = line.split(Constant.SEPARATOR);
    	
    	for (int i = 0; i < words.length; i++) {
    		String word = words[i];
    		//Increment counter value for this word
    		mapWc.add(new Pair(word, 1));
    	}
    	
    	return mapWc;
    }
    
    /**
     * Send key (word) - value (name of the file content), to the master
     * @param unsortedMaps
     * @param fileToShuffle
     * @throws IOException 
     * @throws UnknownHostException 
     */
    private void sendDictionaryElement(List<Pair> unsortedMaps, String fileToShuffle) throws UnknownHostException, IOException {
    	//Get host master and port
		int port_master = 0;
		String host_master = null;
		try {
			port_master = Integer.parseInt(prop.getPropValues(PropertiesReader.MASTER_PORT));
			host_master = prop.getPropValues(PropertiesReader.MASTER_HOST);
		} catch (IOException e) {e.printStackTrace();}
		
        Socket socket = new Socket(host_master, port_master);
        PrintWriter pred = new PrintWriter(socket.getOutputStream());

        for (Pair p : unsortedMaps) {
        	// Send dictionary element
        	pred.println(p.getKey() + Constant.SOCKET_SEPARATOR_MESSAGE + fileToShuffle);
        	pred.flush();
        }

        // Send end message
        pred.println(Constant.SOCKET_END_MESSAGE);
        pred.flush();
        
        pred.close();
        socket.close();
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
    	
    	// Concat data of each files in one
		 try {
             List<Pair> sortedMaps = new ArrayList<Pair>();
             
             // For each files
			 for (int i = 0; i < listFiles.length; i++) {
				 
	             FileReader fic = new FileReader(listFiles[i]);
	             BufferedReader read = new BufferedReader(fic);
	             String line = null;
	
	             // For each lines of the file
	             while ((line = read.readLine()) != null) {
		            String words[] = line.split(Constant.FILE_SEPARATOR);
		            // Search line refers to this key
		            if (words[0].equals(key)) {
			            // Add each line matched with the key to our hashmap
		            	sortedMaps.add(new Pair(words[0], Integer.parseInt(words[1])));
		            }
	             } 
	        	 
	             fic.close();
	             read.close();   
			 }
			 
        	 fileToReduce = Constant.F_SHUFFLING 
        			 + Constant.F_SEPARATOR 
        			 + key
        			 + Constant.F_SEPARATOR 
        			 + InetAddress.getLocalHost().getHostName();
        	 LocalRepoFile.writeFileFromPair(fileToReduce, sortedMaps);
         	
         } catch (IOException e) {
             e.printStackTrace();
         }
    	return fileToReduce;
    	
    }
    
    
    /**
     * Reduce method
     * @param fileToReduce
     */
    public void mappingSortedMaps(String key, String fileToReduce) {
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
            
        	 String fileToAssemble = Constant.F_REDUCING 
        			 + Constant.F_SEPARATOR
        			 + key
        			 + Constant.F_SEPARATOR 
        			 + InetAddress.getLocalHost().getHostName() ;
        	 LocalRepoFile.writeFile(fileToAssemble, finalMaps);
        	 
             fic.close();
             read.close();   
    		 
         } catch (IOException e) {
             e.printStackTrace();
         }
    }
}
