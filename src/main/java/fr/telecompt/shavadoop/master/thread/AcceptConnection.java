//package fr.telecompt.shavadoop.master.thread;
//
//import java.io.*;
//import java.net.*;
//import java.util.Map;
//
//public class AcceptConnection implements Runnable{
//
//	private ServerSocket socketserver = null;
//	private Socket socket = null;
//	public Thread t1;
//	private Map<String, String> dictionary;
//	
//	public AcceptConnection(ServerSocket ss, Map<String, String> _dictionary){
//	 socketserver = ss;
//	 dictionary = _dictionary;
//	}
//	
//	public void run() {
//		
//		try {
//			
//			while(true){
//				
//			socket = socketserver.accept();
//			
//			System.out.println(">>> " + "Un worker veut se connecter" );
//			
//			t1 = new Thread(new ReceiveSlaveInfo(socket, dictionary));
//			t1.start();
//			
//			}
//			
//		} catch (IOException e) {
//			System.err.println("Erreur serveur");
//		}
//		
//	}
//}