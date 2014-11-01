package fr.telecompt.shavadoop.slave;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import fr.telecompt.shavadoop.util.LocalRepoFile;

/**
 * Hello world!
 *
 */
public class Slave 
{
	
	private final String MAP_FUNCTION = "map";
	private final String REDUCE_FUNCTION = "reduce";
	private final static String SEPARATOR = "\\s+"; //TODO util project
	private final static String FILE_SEPARATOR = ",\\s+";
	
    public Slave(String functionName, String fileToTreat) {
    	switch (functionName){
    	case MAP_FUNCTION:
    		//Launch map method
    		splitMapping(fileToTreat);
    		break;
    		//Launch reduce method
    	case REDUCE_FUNCTION:
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
             int nbFile = 1;
             
             while ((line = read.readLine()) != null) {
            	 String fileToShuffle = "UM" + nbFile;
            	 Map<String, Integer> mapWc = wordCount(line);
            	 LocalRepoFile.writeFile(fileToShuffle, mapWc);
            	 ++nbFile;
             }
             
             fic.close();
             read.close();   
    		 
         } catch (IOException e) {
             e.printStackTrace();
         }
    }
    
    /**
     * Count the occurence of each word in the sentence
     * @param line
     * @return
     */
    public Map<String, Integer> wordCount(String line) {
    	Map<String, Integer> mapWc = new HashMap<String, Integer>();
    	//We split the line word by word
    	String words[] = line.split(SEPARATOR);
    	
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
    public void reduce(String fileToReduce) {
		 try {
             FileReader fic = new FileReader(fileToReduce);
             BufferedReader read = new BufferedReader(fic);
             String line = null;

             Map<String, Integer> reduceWc = new HashMap<String, Integer>();
             
             while ((line = read.readLine()) != null) {
            	String words[] = line.split(FILE_SEPARATOR);
         		//Increment counter value for this word
         		if (reduceWc.containsKey(words[0])) {
         			reduceWc.put(words[0], reduceWc.get(words[0]) + Integer.parseInt(words[1]));
         		} else {
         			reduceWc.put(words[0], Integer.parseInt(words[1]));
         		}
             }
             
        	 String fileToAssemble = "RM";
        	 LocalRepoFile.writeFile(fileToAssemble, reduceWc);
        	 
             fic.close();
             read.close();   
    		 
         } catch (IOException e) {
             e.printStackTrace();
         }
    }
}
