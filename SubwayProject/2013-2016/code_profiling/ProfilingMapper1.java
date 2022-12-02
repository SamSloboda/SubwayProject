import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfilingMapper1 extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(",");
        context.getCounter("HOUR_COUNTER", line[1].substring(0,2)).increment(1);
        context.getCounter("CRIME_COUNTER", line[2].toLowerCase()).increment(1);
    }
}