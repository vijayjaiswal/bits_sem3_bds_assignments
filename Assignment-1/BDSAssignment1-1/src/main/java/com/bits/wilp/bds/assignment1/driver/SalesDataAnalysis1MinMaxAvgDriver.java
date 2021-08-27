package com.bits.wilp.bds.assignment1.driver;


import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import com.bits.wilp.bds.assignment1.map.GeoSalesMapper;
import com.bits.wilp.bds.assignment1.partitioner.CountryPartitioner;
import com.bits.wilp.bds.assignment1.reduce.SalesCountryReducer;
import com.bits.wilp.bds.assignment1.util.ApplicationUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Scanner;

// Driver Program for following:
// Q1.	Average unit_price by country for a given item type in a certain year
// Q3.	Find the max and min units_sold in any order for each year by country for a given item type.
//      Use a custom partitioner class instead of default hash based.

public class SalesDataAnalysis1MinMaxAvgDriver extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(SalesDataAnalysis1MinMaxAvgDriver.class);

    // Main Method to Run Historical Sales Data Analysis: 1 Average Unit Price By Country
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new SalesDataAnalysis1MinMaxAvgDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        int result =-1;

        if (args.length != 2) {
            logger.error("Usage: "+getClass().getSimpleName()+" needs two arguments, input and output files path\n");
            System.err.printf("Usage: %s needs two arguments, input and output files path\n", getClass().getSimpleName());
            return -1;
        }
        // Take Input from User
        Scanner sc= new Scanner(System.in); //System.in is a standard input stream.
        System.out.println("1. Finding Average unit_price by country for a given item type in a certain year: ");
        System.out.println("Please enter following:");
        System.out.print("1. (a) Item Type (Baby Food, Beverages, Cereal, Clothes, Cosmetics\n" +
                "\t\t\t\t Fruits, Household, Meat, Office Supplies, Personal Care, Snacks, Vegetables): ");

        String strItemType= sc.nextLine();

        // Deleting existing Output folder (if already exist)
        logger.info("Deleting existing Output folder: "+args[1]);
        FileUtils.deleteDirectory(new File(args[1]));

        // Configure MapReduce Job
        Job job = Job.getInstance();
        job.setJarByClass(SalesDataAnalysis1MinMaxAvgDriver.class);
        job.setJobName("SalesDataAnalysis-1-MinMaxAveragePriceByCountry");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GeoSalesOrder.class);

        // Setting User Input into Job context
        job.getConfiguration().set(ApplicationUtils.INPUT_ITEM_TYPE,strItemType);

        // Setting Job Input File path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Setting Job Output File path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Setting Partition by Country
        job.setPartitionerClass(CountryPartitioner.class);
        job.setNumReduceTasks(2);

        // Configuring Mapper and Reducer classes
        job.setMapperClass(GeoSalesMapper.class);
        job.setReducerClass(SalesCountryReducer.class);

        result = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            logger.info("Job was successful");
        } else if(!job.isSuccessful()) {
            logger.info("Job was not successful");
        }

        return result;
    }
}
