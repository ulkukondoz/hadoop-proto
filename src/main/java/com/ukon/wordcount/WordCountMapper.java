package com.ukon.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, IntWritable>
{
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException
	{
		String line = text.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens())
		{
			String nextToken = tokenizer.nextToken();
			System.out.println("--- MAPPER - next token : " + nextToken);
			word.set(nextToken);


			outputCollector.collect(word, one);

		}
	}
}
