package edu.bufflo.sem2.dic.lab4;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class With3Grams {

	public static Map<String, ArrayList<String>> StringMat = new HashMap<String, ArrayList<String>>();
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
		int i = 0;
		try {
			br = new BufferedReader(new FileReader(csvFile));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		try {

			while ((null != br) && (line = br.readLine()) != null) {

				String[] country = line.split(cvsSplitBy);
				String key = normalise(country[0]);
				if (StringMat.containsKey(key)) {
					list = StringMat.get(key);
					list.add(normalise(country[2]));
					StringMat.put(key, list);
				} else {
					list = new ArrayList<String>();
					list.add(normalise(country[2]));
					StringMat.put(normalise(country[0]), list);
				}
				list.clear();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word;
		private Text locText = new Text();
		private List<Text> wordList = new ArrayList<Text>();
		ArrayList<String> lemmaList1 = new ArrayList<String>();
		ArrayList<String> lemmaList2 = new ArrayList<String>();
		ArrayList<String> lemmaList3 = new ArrayList<String>();
		int i, j, m, n, k, q;
		String f, s, p;

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
				wordList.clear();
				for (int i = 0; i < tokens.length; i++) {
					word = new Text();
					word.set(tokens[i]);
					wordList.add(word);
				}
			}

			for (i = 0; i < wordList.size(); i++) {

				for (j = 0; j < wordList.size(); j++)

				{
					for (k = 0; k < wordList.size(); k++)

					{
						if (i != j) {

							f = wordList.get(i).toString();
							s = wordList.get(j).toString();
							p = wordList.get(k).toString();
							lemmaList1.clear();
							lemmaList2.clear();
							lemmaList3.clear();
							if ((null != f && !"".equals(f)) && (null != s && !"".equals(s))) {
								checkLemma(lemmaList1, normalise(f));
								checkLemma(lemmaList2, normalise(s));
								checkLemma(lemmaList3, normalise(s));
							}

							for (m = 0; m < lemmaList1.size(); m++) {
								for (n = 0; n < lemmaList2.size(); n++) {
									for (q = 0; q < lemmaList3.size(); q++) {
										String v = lemmaList1.get(m) + "," + lemmaList2.get(n) + "," + lemmaList3.get(q);
										word.set(v);
										locText.set(loc);
										context.write(word, locText);
									}
								}
							}

						}
					}
				}
			}
		}

		private void checkLemma(ArrayList<String> al, String tex) {

			al.add(tex);
			if (StringMat.containsKey(tex)) {
				al.addAll(StringMat.get(tex));
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
		Job job = new Job(conf, "Lemma-3grams");

		// job. (conf, "word count");
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
