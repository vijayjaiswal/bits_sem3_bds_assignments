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
        System.out.print("Q2. total units_sold by year for a given country and a given item type: ");
        System.out.print("Please enter following: ");
        System.out.print("1. (a) Enter Country \n (Afghanistan, Albania, Algeria, Andorra, Angola, Antigua and Barbuda , \n" +
                "Armenia, Australia, Austria, Azerbaijan, Bahrain, Bangladesh, Barbados, Belarus, Belgium, Belize, \n" +
                "Benin, Bhutan, Bosnia and Herzegovina, Botswana, Brunei, Bulgaria, Burkina Faso, Burundi, Cambodia, \n" +
                "Cameroon, Canada, Cape Verde, Central African Republic, Chad, China, Comoros, Costa Rica, \n" +
                "Cote d'Ivoire, Croatia, Cuba, Cyprus, Czech Republic, Democratic Republic of the Congo, Denmark, \n" +
                "Djibouti, Dominica, Dominican Republic, East Timor, Egypt, El Salvador, Equatorial Guinea, \n" +
                "Eritrea, Estonia, Ethiopia, Federated States of Micronesia, Fiji, Finland, France, Gabon, Georgia, \n" +
                "Germany, Ghana, Greece, Greenland, Grenada, Guatemala, Guinea, Guinea-Bissau, Haiti, Honduras, \n" +
                "Hungary, Iceland, India, Indonesia, Iran, Iraq, Ireland, Israel, Italy, Jamaica, Japan, Jordan, \n" +
                "Kazakhstan, Kenya, Kiribati, Kosovo, Kuwait, Kyrgyzstan, Laos, Latvia, Lebanon, Lesotho, Liberia, \n" +
                "Libya, Liechtenstein, Lithuania, Luxembourg, Macedonia, Madagascar, Malawi, Malaysia, Maldives, Mali, \n" +
                "Malta, Marshall Islands, Mauritania, Mauritius , Mexico, Moldova , Monaco, Mongolia, Montenegro, \n" +
                "Morocco, Mozambique, Myanmar, Namibia, Nauru, Nepal, Netherlands, New Zealand, Nicaragua, Niger, \n" +
                "Nigeria, North Korea, Norway, Oman, Pakistan, Palau, Panama, Papua New Guinea, Philippines, Poland, \n" +
                "Portugal, Qatar, Republic of the Congo, Romania, Russia, Rwanda, Saint Kitts and Nevis , Saint Lucia, \n" +
                "Saint Vincent and the Grenadines, Samoa , San Marino, Sao Tome and Principe, Saudi Arabia, Senegal, \n" +
                "Serbia, Seychelles , Sierra Leone, Singapore, Slovakia, Slovenia, Solomon Islands, Somalia, \n" +
                "South Africa, South Korea, South Sudan, Spain, Sri Lanka, Sudan, Swaziland, Sweden, Switzerland, \n" +
                "Syria, Taiwan, Tajikistan, Tanzania, Thailand, The Bahamas, The Gambia, Togo, Tonga, \n" +
                "Trinidad and Tobago, Tunisia , Turkey, Turkmenistan, Tuvalu, Uganda, Ukraine, United Arab Emirates, \n" +
                "United Kingdom, United States of America, Uzbekistan, Vanuatu, Vatican City, Vietnam, Yemen, Zambia, Zimbabwe): ");
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
        //job.setPartitionerClass(CountryPartitioner.class);
        //job.setNumReduceTasks(1);

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
