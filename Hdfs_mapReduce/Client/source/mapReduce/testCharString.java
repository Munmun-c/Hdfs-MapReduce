package mapReduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

public class testCharString {

	public testCharString() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String aString = "abcde\nefghij\n123";
		byte x[] = aString.getBytes();
		String t = new String(x);
		System.out.println("And the rumble");
		BufferedReader reader = new BufferedReader(new StringReader(t));
		String line;
		try {
			while ((line = reader.readLine()) != null) {
			      
				 System.out.println(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
