package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProductReviewWritable  implements Writable {
    private String productId;
    private double rating;

    public ProductReviewWritable() {
    }

    public ProductReviewWritable(String productId, double rating) {
        this.productId = productId;
        this.rating = rating;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.productId);
        dataOutput.writeDouble(this.rating);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.productId = dataInput.readUTF();
        this.rating = dataInput.readDouble();
    }
}
