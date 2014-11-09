package fr.telecompt.shavadoop.slave;

public class Pair
{
   private String key;
   private int value;

   public Pair(String key, int value)
   {
      this.key = key;
      this.value = value;
   }

	public String getKey() {
		return key;
	}
	
	public int getValue() {
		return value;
	}
   
}
