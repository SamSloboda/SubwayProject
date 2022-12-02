import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfilingReducer1 extends Reducer<NullWritable, Text, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int crimeCount = 0;
        String crimeDescription = null;
        for (Text val : values) {
            if (crimeCount == 0) {
                crimeDescription = val.toString().split(",")[0];
            }
            crimeCount += 1;
        }
        context.write(key, new Text(crimeDescription + ", Count: " + crimeCount));
    }

}
