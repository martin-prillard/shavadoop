package fr.telecompt.shavadoop.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
		catch (Exception e){
			System.out.println(e.toString());
		}
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
		catch (Exception e){
			System.out.println(e.toString());
		}
	}
	
	public static void writeFileFromPair(String nameFile, List<Pair> content) {
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
