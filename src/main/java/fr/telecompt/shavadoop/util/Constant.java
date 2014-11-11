package fr.telecompt.shavadoop.util;

public class Constant {

	// Shavadoop
	public final static String APP_VERSION = "v0.3";
	public final static String APP_EXTENSION = ".jar";
	public final static String APP_JAR = "shavadoop" + "_" + APP_VERSION + APP_EXTENSION;
	public final static String USERNAME = System.getProperty("user.name");
	
	// Path directory
	public static String PATH_SLAVE = new PropReader().getPropValues(PropReader.PATH_SLAVE);
	public static String PATH_MASTER = new PropReader().getPropValues(PropReader.PATH_MASTER);
	public final static String PATH_REPO_RES = PATH_MASTER + "/temp/";
	public final static String PATH_JAR = PATH_MASTER + "/" + APP_JAR;
	public final static String PATH_NETWORK_IP_DEFAULT_FILE = "res/ip_adress";
	public static String PATH_NETWORK_IP_FILE = PATH_NETWORK_IP_DEFAULT_FILE;
	public final static String PATH_DSA_DEFAULT_FILE = "/.ssh/id_dsa";
	// Path files
	public final static String PATH_F_INPUT_CLEANED = PATH_REPO_RES + "input_cleaned";
	public final static String PATH_F_SPLITING = PATH_REPO_RES + "S";
	public final static String PATH_F_MAPPING = PATH_REPO_RES + "UM";
	public final static String PATH_F_SHUFFLING_DICTIONARY = PATH_REPO_RES + "DSM";
	public final static String PATH_F_SHUFFLING = PATH_REPO_RES + "SM";
	public final static String PATH_F_REDUCING = PATH_REPO_RES + "RM";
	public final static String PATH_F_FINAL_RESULT = PATH_REPO_RES + "output";
	
	// Mode
	public final static boolean MODE_DEBUG = Boolean.parseBoolean(new PropReader().getPropValues(PropReader.MODE_DEBUG));
	public final static boolean MODE_SCP_FILES = Boolean.parseBoolean(new PropReader().getPropValues(PropReader.MODE_SCP_FILES));
	public final static String APP_DEBUG_TITLE = "-------------------------------------------------------";
	
	// Separator
	public final static String SEP_NAME_FILE = "_";
	public final static String SEP_WORD = " ";
	public final static String SEP_CONTAINS_FILE = ", ";
	public final static String SEP_FILES_SHUFFLING_MAP = ",";
	public final static String SEP_FILES_SHUFFLING_MAP_GROUP = ";";
	
	// Socket
	public final static String SEP_SOCKET_MESSAGE = ";";
	public final static String END_SOCKET_MESSAGE = "END";
	
	// Job
	public final static int THREAD_LIFETIME = Integer.parseInt(new PropReader().getPropValues(PropReader.THREAD_LIFETIME));
	
}
