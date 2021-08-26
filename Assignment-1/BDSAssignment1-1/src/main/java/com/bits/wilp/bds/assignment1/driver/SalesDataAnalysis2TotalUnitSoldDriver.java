package com.bits.wilp.bds.assignment1.driver;


import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import com.bits.wilp.bds.assignment1.map.GeoItemTypeSalesMapper;
import com.bits.wilp.bds.assignment1.map.GeoSalesMapper;
import com.bits.wilp.bds.assignment1.partitioner.CountryPartitioner;
import com.bits.wilp.bds.assignment1.reduce.SalesCountryReducer;
import com.bits.wilp.bds.assignment1.reduce.SalesItemTypeCountryReducer;
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

public class SalesDataAnalysis2TotalUnitSoldDriver extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(SalesDataAnalysis2TotalUnitSoldDriver.class);

    // Main Method to Run Historical Sales Data Analysis: 1 Average Unit Price By Country
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new SalesDataAnalysis2TotalUnitSoldDriver(), args);
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
        System.out.print("2. total units_sold by year for a given country and a given item type: ");
        System.out.print("Please enter following: ");
        System.out.print("1. (a) Year (Bhutan, Cambodia): ");
        String strCountry= sc.nextLine();
        System.out.print("2. (b) Item Type (Baby Food, Beverages, Cereal, Clothes, Cosmetics\n" +
                "\t\t\t\t Fruits, Household, Meat, Office Supplies, Personal Care, Snacks, Vegetables): ");
        String strItemType= sc.nextLine();

        // Deleting existing Output folder (if already exist)
        logger.info("Deleting existing Output folder: "+args[1]);
        FileUtils.deleteDirectory(new File(args[1]));

        // Configure MapReduce Job
        Job job = Job.getInstance();
        job.setJarByClass(SalesDataAnalysis2TotalUnitSoldDriver.class);
        job.setJobName("SalesDataAnalysis-2-TotalUnitsSoldByCountryAndItemType");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GeoSalesOrder.class);

        // Setting User Input into Job context
        job.getConfiguration().set(ApplicationUtils.INPUT_ITEM_TYPE,strItemType);
        job.getConfiguration().set(ApplicationUtils.INPUT_COUNTRY, strCountry);

        // Setting Job Input File path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Setting Job Output File path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Setting Partition by Country
        job.setPartitionerClass(CountryPartitioner.class);
        job.setNumReduceTasks(2);

        // Configuring Mapper and Reducer classes
        job.setMapperClass(GeoItemTypeSalesMapper.class);
        job.setReducerClass(SalesItemTypeCountryReducer.class);

        result = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            logger.info("Job was successful");
        } else if(!job.isSuccessful()) {
            logger.info("Job was not successful");
        }

        return result;
    }
}
