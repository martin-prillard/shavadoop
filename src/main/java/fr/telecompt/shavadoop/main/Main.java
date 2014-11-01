package fr.telecompt.shavadoop.main;

import fr.telecompt.shavadoop.master.Master;

public class Main {

    public static void main( String[] args )
    {
    	if (args.length == 3) {
    		new Master(args[0], args[1], args[2]);
    	} else {
    		System.out.println("Not enough args");
    	}
    }
    
}
