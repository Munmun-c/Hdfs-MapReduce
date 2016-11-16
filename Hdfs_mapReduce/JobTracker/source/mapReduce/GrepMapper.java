package mapReduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import static mapReduce.MRConstants.*;;

public class GrepMapper implements IMapper{
	
	String matchString;
	public GrepMapper() {
		BufferedReader br;
		// TODO Auto-generated constructor stub
		try {
			br = new BufferedReader(new FileReader(GREP_STRING_FILE));
			try {
				matchString = br.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

	@Override
	public String map(String mapInp) {
		// TODO Auto-generated method stub
		if (mapInp.contains(matchString)) {
			return mapInp+":1";
		}
		else {
			return null;
		}
	}

}
