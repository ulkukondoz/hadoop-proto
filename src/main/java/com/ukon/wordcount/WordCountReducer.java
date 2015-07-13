package com.ukon.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable>
{
	public void reduce(Text key, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException
	{
		int sum = 0;
		while (iterator.hasNext())
		{
			int occurences = iterator.next().get();
			System.out.println("REDUCER - key : " + key + " occurences : " + occurences);
			sum += occurences;
			System.out.println("REDUCER - sum : " + sum);
		}

        outputCollector.collect(key, new IntWritable(sum));
	}
}
