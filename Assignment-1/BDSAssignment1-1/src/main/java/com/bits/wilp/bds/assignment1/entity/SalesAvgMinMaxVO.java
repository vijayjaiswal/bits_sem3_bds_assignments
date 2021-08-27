package com.bits.wilp.bds.assignment1.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// POJO: Entity to hold and emmit Average Unit Price, Min and Max of Unit Sold
public class SalesAvgMinMaxVO implements Writable {
    private Double averageUnitPrice; // average unit_price
    private Double minUnitSold; // min UnitPrice
    private Double maxUnitSold; // max UnitPrice

    public Double getAverageUnitPrice() {
        return averageUnitPrice;
    }

    public void setAverageUnitPrice(Double averageUnitPrice) {
        this.averageUnitPrice = averageUnitPrice;
    }

    public Double getMinUnitSold() {
        return minUnitSold;
    }

    public void setMinUnitSold(Double minUnitSold) {
        this.minUnitSold = minUnitSold;
    }

    public Double getMaxUnitSold() {
        return maxUnitSold;
    }

    public void setMaxUnitSold(Double maxUnitSold) {
        this.maxUnitSold = maxUnitSold;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.NO_CLASS_NAME_STYLE);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(averageUnitPrice);
        dataOutput.writeDouble(minUnitSold);
        dataOutput.writeDouble(maxUnitSold);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        averageUnitPrice = dataInput.readDouble();
        minUnitSold = dataInput.readDouble();
        maxUnitSold = dataInput.readDouble();
    }
}
