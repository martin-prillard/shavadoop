package fr.telecompt.shavadoop.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

public class LocalRepoFile {
	
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
			}
			catch (Exception e){
				System.out.println(e.toString());
			}
      	 }
	}
	
	public static void createDirectory(File file) {
	    // if the directory does not exist, create it
	    if (!file.exists()) {
	     try{
	    	 file.mkdir();
	       } catch(Exception e){e.printStackTrace();}  
	    }
	}
	
	public static void cleanDirectory(File file) {
		try {
			FileUtils.cleanDirectory(file);
		} catch (IOException e) {e.printStackTrace();}
	}
}
