package edu.bufflo.sem2.dic.lab4;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Lemma {
	public static Map<String,ArrayList<String>> StringMat = new HashMap<String,ArrayList<String>>();
	public static ArrayList<String> list;
	public static String normalise(String tex) {
		tex = tex.replace('j', 'i');
		tex = tex.replace('v', 'u');
		return tex;
	}

	public static void parseCSV(String csvFile) {
		// String csvFile = "/home/hadoop/Downloads/lab
		// 4/text_one_and_two_and_lemmatizer/Book5.csv";
		String line = "";
		String cvsSplitBy = ",";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(csvFile));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		try {

			while ((null != br) && (line = br.readLine()) != null) {
				
				String[] country = line.split(cvsSplitBy);
				String key = normalise(country[0]);
				if(StringMat.containsKey(key))
				{
					list = StringMat.get(key);
					list.add(normalise(country[2]));
						StringMat.put(key,list );
				}
				else
				{
					list = new ArrayList<String>();
					list.add(normalise(country[2]));
					StringMat.put(normalise(country[0]),list);
				}
				list.clear();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text locText = new Text();
		List<String> lemmaList = new ArrayList<String>();
		Iterator<String> lemmaIter;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String text = value.toString();
			String loc = null;
			try {
				int locInd = text.indexOf('>');
				loc = text.substring(0, locInd + 1);
				text = text.substring(locInd + 2);
				text = text.replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+", " ");
			} catch (Exception e) {
			}
			String[] tokens = text.split("\\s+");

			if (tokens.length > 1) {
				lemmaList.clear();
				for (int i = 0; i < tokens.length; i++) {
					String tex = tokens[i];
					if (null != tex && !"".equals(tex)) {
						checkLemma(normalise(tex));
					}
					
					lemmaIter = lemmaList.iterator();

					while (lemmaIter.hasNext()) {
						String v = lemmaIter.next();
						word.set(v);
						locText.set(loc);
						context.write(word, locText);
					}

				}
			}

		}

		private void checkLemma(String tex) {

			lemmaList.add(tex);
			if(StringMat.containsKey(tex)) {
				lemmaList.addAll(StringMat.get(tex));
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sumLoc = new StringBuilder();
			for (Text val : values) {
				sumLoc = sumLoc.append(val.toString());
				sumLoc = sumLoc.append(",");
			}
			String ff = sumLoc.toString();
			if (ff.endsWith(","))
				ff = ff.substring(0, (ff.length()) - 1);
			result.set(ff);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		parseCSV(args[2]);
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Lemma");
		job.setJarByClass(Lemma.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
