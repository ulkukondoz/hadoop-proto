package com.ukon.shopping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Shop {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("", "");
        conf.set("input-set", "");

        Job job = new Job(conf);
        job.setJobName("shop-transaction");

//        job.setJarByClass(TransactionMapper.class);
//        job.setJarByClass(UserMapper.class);

        job.setReducerClass(ShoppingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("shopping-user-input-set.txt"), TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, new Path("shopping-transaction-input-set.txt"), TextInputFormat.class, TransactionMapper.class);

        FileOutputFormat.setOutputPath(job, new Path("shopping-output2-set.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
