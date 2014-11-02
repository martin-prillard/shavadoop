package fr.telecompt.shavadoop.slave;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.LocalRepoFile;

/**
 * Hello world!
 *
 */
public class Slave 
{
	
	private final String MAP_FUNCTION = "map";
	private final String SHUFFLING_MAP_FUNCTION = "shuffling_map_function";
	
	public Slave(){}
	
    public Slave(String functionName, String fileToTreat) {
    	switch (functionName){
    	case MAP_FUNCTION:
    		//Launch map method
    		splitMapping(fileToTreat);
    		break;
    	case SHUFFLING_MAP_FUNCTION:
    		//Lanch shuffling map method
    		String fileToReduce = shufflingMaps(fileToTreat);
    		//Launch reduce method
    		mappingSortedMaps(fileToReduce);
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
             int nbLine = 1;
             
             Map<String, Integer> unsortedMaps = null;
            		 
             while ((line = read.readLine()) != null) {
            	 unsortedMaps = wordCount(unsortedMaps, line);
            	 ++nbLine;
             }
             
             fic.close();
             read.close();   
    		 
             // Write UM File
        	 String fileToShuffle = "UM" + nbLine;
        	 LocalRepoFile.writeFile(fileToShuffle, unsortedMaps);
        	 // Send dictionary with UNIQUE key (word) and hostname to the master
        	 //TODO
        	 
         } catch (IOException e) {
             e.printStackTrace();
         }
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
    
    
    public void combiner() {} //TODO

    
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
             
        	 String fileToAssemble = "RM";
        	 LocalRepoFile.writeFile(fileToAssemble, finalMaps);
        	 
             fic.close();
             read.close();   
    		 
         } catch (IOException e) {
             e.printStackTrace();
         }
    }
    
    //Group and sort maps results by key
    public String shufflingMaps(String filesToTreat) {
    	// Final file to reduce
    	String fileToReduce = null;
    	// Get the list of file
    	String[] listFiles = filesToTreat.split(Constant.FILES_SHUFFLING_MAP_SEPARATOR);
    	//TODO search files from NFS
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
		            // Add each line to our hashmap
	            	sortedMaps.put(words[0], Integer.parseInt(words[1]));
	             } 
	        	 
	             fic.close();
	             read.close();   
			 }
			 
        	 String fileToAssemble = "SM";
        	 LocalRepoFile.writeFile(fileToAssemble, sortedMaps);
        	 
         } catch (IOException e) {
             e.printStackTrace();
         }
    	return fileToReduce;
    	
    }
}
