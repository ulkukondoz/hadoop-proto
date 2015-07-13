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

        Job jobTransaction = new Job(conf);
        jobTransaction.setJobName("shop-transaction");

        jobTransaction.setJarByClass(TransactionMapper.class);
        jobTransaction.setReducerClass(ShoppingReducer.class);

        jobTransaction.setMapOutputKeyClass(Text.class);
        jobTransaction.setOutputValueClass(Text.class);


        jobTransaction.setOutputKeyClass(Text.class);
        jobTransaction.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(jobTransaction, new Path("shopping-user-input-set.txt"), TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(jobTransaction, new Path("shopping-transaction-input-set.txt"), TextInputFormat.class, TransactionMapper.class);

        FileOutputFormat.setOutputPath(jobTransaction, new Path("shopping-output2-set.txt"));

        System.exit(jobTransaction.waitForCompletion(true) ? 0 : 1);

    }

}
