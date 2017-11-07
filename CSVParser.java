package edu.bufflo.sem2.dic.lab4;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CSVParser {
	private static String StringMat[][];
	
	public static void main(String[] args) {
		//StringMat = parseCSV();
		String tex = "Rimji, is, a. good! virgvvin jmbrella";
		tex = tex.replace('j', 'i');
		tex = tex.replace('v', 'u');
		tex = tex.replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+", " ");
		System.out.println(tex);
		String[] tokens = tex.split(" ");
		for(String v : tokens)
			System.out.println(v);
	}
	
	public static String[][] parseCSV()
	{
		  String csvFile = "C:\\Users\\RIMI\\Documents\\Book5.csv";
	        String line = "";
	        String cvsSplitBy = ",";
	        BufferedReader br = null;
	        int i =0;
	        StringMat = new String[10][3];
			try {
				br = new BufferedReader(new FileReader(csvFile));
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	        try  {
	        	System.out.println("hiii");
	            while ((null!= br) && (line = br.readLine()) != null) {
	            	System.out.println("hey");
	                // use comma as separator
	            	System.out.println(line);
	                String[] country = line.split(cvsSplitBy);
	                for(int j = 0;j<country.length;j++)
	                {
	                StringMat[i][j] = country[j];
	                System.out.println(StringMat[i][j]);
	                }
	                i++;
	                
	               // System.out.println("Country [code= " + country[4] + " , name=" + country[5] + "]");

	            }

	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        return StringMat;
	}

}
