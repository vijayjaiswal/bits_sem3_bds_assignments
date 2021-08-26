package com.bits.wilp.bds.assignment1.util;

import com.bits.wilp.bds.assignment1.entity.GeoSalesOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Objects;

public class ApplicationUtils {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationUtils.class);

    public static final String INPUT_ITEM_TYPE = "inputItemType";
    public static final String INPUT_SALE_YEAR = "inputSaleYear";
    public static final String INPUT_COUNTRY = "inputCountry";


    private static final String TIMESTAMP_FORMAT_STR = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat(TIMESTAMP_FORMAT_STR);

    // Utility method for converting String into Date having format "yyyy-MM-dd HH:mm:ss"
    public static Date getDateFromString(String value){

        Date date = null;
        try {
            date = TIMESTAMP_FORMAT.parse(value);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }
        return date;
    }
    //  Utility method for getting date part from Date object
    public static String getDatePartFromDate(Date value) {
        String result = "1000";
        if (Objects.nonNull(value)) {
            Calendar calendar = new GregorianCalendar();
            calendar.setTime(value);
            result = ""+calendar.get(Calendar.YEAR);
        }
        return result;
    }

    // Utility method for converting String into Integer
    public static Integer getIntegerFromString(String value) {
        Integer intValue = null;
        try {
            intValue = Integer.parseInt(value);
        } catch (NumberFormatException  e) {
            System.out.println(e.getMessage());
            intValue =new Integer(0);
        }
        return intValue;
    }

    // Utility method for converting String into Double
    public static Double getDoubleFromString(String value) {
        Double doubleValue = null;
        try {
            doubleValue = Double.parseDouble(value);
        } catch (NumberFormatException  e) {
            System.out.println(e.getMessage());
            doubleValue =new Double(0);
        }
        return doubleValue;
    }

    // Utility method for converting String into GeoSalesOrder
    public static GeoSalesOrder createGeoSalesOrder(String saleData){
        //logger.info("saleData:"+saleData);
        GeoSalesOrder geoSalesOrder= new GeoSalesOrder();

        //Split the input data into rows first
        //String[] singleCountrySalesData= saleData.tokenize();
        String[] singleCountrySalesData = saleData.split(",",-1);
        if("index".equalsIgnoreCase(singleCountrySalesData[0])) {
            // skip
            //logger.info("Skipping Header Row..");
        }
        else {

            //Process each line separately
            // index	region	country	item_type	sales_channel	order_priority	order_date	order_id
            // ship_date	units_sold	unit_price	unit_cost	total_revenue	total_cost	total_profit
            geoSalesOrder.setIndex(singleCountrySalesData[0]);
            //region
            geoSalesOrder.setRegion(singleCountrySalesData[1]);
            //country
            geoSalesOrder.setCountry(singleCountrySalesData[2]);
            //item_type
            geoSalesOrder.setItemType(singleCountrySalesData[3]);
            //sales_channel
            geoSalesOrder.setSalesChannel(singleCountrySalesData[4]);
            //order_priority
            geoSalesOrder.setOrderPriority(singleCountrySalesData[5]);
            // order_date (String)
            geoSalesOrder.setStrOrderDate(singleCountrySalesData[6]);
            // order_id
            geoSalesOrder.setOrderId(singleCountrySalesData[7]);
            // ship_date
            geoSalesOrder.setStrShipDate(singleCountrySalesData[8]);
            // units_sold
            geoSalesOrder.setUnitsSold(getIntegerFromString(singleCountrySalesData[9]));
            // unit_price
            geoSalesOrder.setUnitPrice(getDoubleFromString(singleCountrySalesData[10]));
            // unit_cost
            geoSalesOrder.setUnitCost(getDoubleFromString(singleCountrySalesData[11]));
            // total_revenue
            geoSalesOrder.setTotalRevenue(getDoubleFromString(singleCountrySalesData[12]));
            // total_cost
            geoSalesOrder.setTotalCost(getDoubleFromString(singleCountrySalesData[13]));
            // total_profit
            geoSalesOrder.setTotalProfit(getDoubleFromString(singleCountrySalesData[14]));
        }
        return geoSalesOrder;
    }
}
