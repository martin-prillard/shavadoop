package fr.telecompt.shavadoop.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import fr.telecompt.shavadoop.slave.Pair;

public class Util {

	/**
	 * Create file from map
	 * @param nameFile
	 * @param content
	 */
	public static void writeFile(String nameFile, Map<String, Integer> content) {
		try {
			FileWriter fw = new FileWriter (nameFile);
			BufferedWriter bw = new BufferedWriter (fw);
			PrintWriter write = new PrintWriter (bw); 
			
			for(Entry<String, Integer> entry : content.entrySet()) {
			    write.println (entry.getKey()
			    		+ Constant.FILE_SEPARATOR
			    		+ entry.getValue()); 
			}
			
			write.close();
			bw.close();
			fw.close();
		}
		catch (Exception e){e.printStackTrace();}
	}
	
	public static void writeFile(String nameFile, List<String> content) {
		try {
			FileWriter fw = new FileWriter (nameFile);
			BufferedWriter bw = new BufferedWriter (fw);
			PrintWriter write = new PrintWriter (bw); 
			
			for(String line : content) {
			    write.println (line); 
			}
			
			write.close();
			bw.close();
			fw.close();
		}
		catch (Exception e){e.printStackTrace();}
	}
	
	public static void writeFileFromPair(String nameFile, List<Pair> content) {
      	 // If the file exist, we concat
      	 if(new File(nameFile).exists()) {
      		try(
      			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(nameFile, true)))) {
				for(Pair p : content) {
					out.println (p.getKey()
				    		+ Constant.FILE_SEPARATOR
				    		+ p.getValue()); 
				}
      		}catch (Exception e) {e.printStackTrace();}
      	 // we create the file
      	 } else {
			try {
				FileWriter fw = new FileWriter (nameFile);
				BufferedWriter bw = new BufferedWriter (fw);
				PrintWriter write = new PrintWriter (bw); 
				
				for(Pair p : content) {
				    write.println (p.getKey()
				    		+ Constant.FILE_SEPARATOR
				    		+ p.getValue()); 
				}
				
				write.close();
				bw.close();
				fw.close();
			}
			catch (Exception e){
				System.out.println(e.toString());
			}
      	 }
	}
	
	/**
	 * Create a directory
	 * @param file
	 */
	public static void createDirectory(File file) {
	    // if the directory does not exist, create it
	    if (!file.exists()) {
	     try{
	    	 file.mkdir();
	       } catch(Exception e){e.printStackTrace();}  
	    }
	}
	
	/**
	 * Clean the directory
	 * @param file
	 */
	public static void cleanDirectory(File file) {
		try {
			FileUtils.cleanDirectory(file);
		} catch (IOException e) {e.printStackTrace();}
	}
	
	public static String listToString(Set<String> input) {
		String res = "";
		for (String file : input) {
			res += file + Constant.FILES_SHUFFLING_MAP_SEPARATOR;
		}
		
		// Remove the last separator
	    if (res.length() > 0 && Character.toString(res.charAt(res.length()-1)).equals(Constant.FILES_SHUFFLING_MAP_SEPARATOR)) {
	    	res = res.substring(0, res.length()-1);
	    }
	    
		return res;
	}
	
    /**
     * Clean the line
     * @param line
     * @return line clean
     */
    private static String cleanLine(String line) {
    	String clean = line;
    	clean = clean.trim();
    	// clean the non alpha numeric character or space
    	clean = clean.replaceAll("[^a-zA-Z0-9\\s]", "");
    	// just one space beetween each words
    	clean = clean.replaceAll("\\s+", " ");
    	clean = clean.replaceAll("\\t+", " ");
    	return clean;
    }
    
	public static ExecutorService fixedThreadPoolWithQueueSize(int nThreads, int queueSize) {
	    return new ThreadPoolExecutor(nThreads, nThreads,
	                                  5000L, TimeUnit.MILLISECONDS,
	                                  new ArrayBlockingQueue<Runnable>(queueSize, true), 
	                                  new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	
	/**
	 * Clean a text
	 * @param fileInput
	 * @param fileOutput
	 */
	public static void cleanText(String fileInput, String fileOutput) {
		FileReader fr;
		try {
			fr = new FileReader(fileInput);
			BufferedReader br = new BufferedReader(fr); 
			FileWriter fw = new FileWriter(fileOutput); 
			BufferedWriter bw = new BufferedWriter (fw);
			PrintWriter write = new PrintWriter(bw); 
			String line;
	
			while((line = br.readLine()) != null)
			{ 
			    line = cleanLine(line);
			    // don't write out blank lines
			    if (!line.equals("") || !line.isEmpty()) 
			    {
			        write.println(line);
			    }
			} 
			write.close();
			br.close();
			fr.close();
		} catch (Exception e) {e.printStackTrace();} 
		
	}
	
}
