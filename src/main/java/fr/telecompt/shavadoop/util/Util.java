package fr.telecompt.shavadoop.util;

import java.util.List;

public class Util {

	public static String listToString(List<String> input) {
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
}
