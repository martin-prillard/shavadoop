package fr.telecompt.shavadoop.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Nfs {

	public static void getFileFromNFS(String nameFile) {
		// TODO
	}
	
	public static void postFileToNFS(String nameFile, List<String> content) {
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
	
	public static void postFileToNFS(String nameFile, Map<String, Integer> content) {
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
}
