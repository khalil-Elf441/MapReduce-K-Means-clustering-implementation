import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class BaryWritable extends PointWritable implements WritableComparable<BaryWritable>
{
    private int clusterId;

    public BaryWritable()
    {
        super();
    }

    public BaryWritable(int clusterId, double[] coordinates)
    {
        super(coordinates);
        this.clusterId = clusterId;
    }

    public int getClusterId()
    {
        return this.clusterId;
    }

    public void add(PointWritable point)
    {
        double[] otherCoords = point.getCoordinates();
        // ajoute les coordonnées du point à celles de this
        for(int i = 0; i < this.coordinates.length; ++i)
            this.coordinates[i] += otherCoords[i];
    }

    public void divideBy(int size)
    {
        // divise les coords de this par la valeur size
        for(int i = 0; i < this.coordinates.length; ++i)
            this.coordinates[i] /= (double)size;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(this.clusterId);
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.clusterId = in.readInt();
        super.readFields(in);
    }

    @Override
    public int compareTo(BaryWritable other)
    {
        final int otherClusterId = other.getClusterId();
        if(this.clusterId < otherClusterId)
            return -1;
        else if(this.clusterId == otherClusterId)
            return 0;
        else // if this.clusterId > otherClusterId
            return 1;
    }

    @Override
    public boolean equals(Object other)
    {
        return this.compareTo((BaryWritable)other) == 0;
    }
    
    @Override
    public String toString()
    {
        return String.valueOf(this.clusterId);
    }
}
