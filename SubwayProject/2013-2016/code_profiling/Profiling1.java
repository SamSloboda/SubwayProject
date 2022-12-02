import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

public class Profiling1 {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Profiling1 <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Profiling1.class);
        job.setJobName("Profiling1");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ProfilingMapper1.class);
        //job.setReducerClass(ProfilingReducer1.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Print counters
        Counters counters = job.getCounters();

        System.out.println("Hours Counter");
        CounterGroup dynamicGroup = counters.getGroup("HOUR_COUNTER");
        for (Counter counter : dynamicGroup) {
            switch (counter.getDisplayName().toLowerCase()) {
                case "00":
                    System.out.println("00: " + counter.getValue());
                    break;
                case "01":
                    System.out.println("01: " + counter.getValue());
                    break;
                case "02":
                    System.out.println("02: " + counter.getValue());
                    break;
                case "03":
                    System.out.println("03: " + counter.getValue());
                    break;
                case "04":
                    System.out.println("04: " + counter.getValue());
                    break;
                case "05":
                    System.out.println("05: " + counter.getValue());
                    break;
                case "06":
                    System.out.println("06: " + counter.getValue());
                    break;
                case "07":
                    System.out.println("07: " + counter.getValue());
                    break;
                case "08":
                    System.out.println("08: " + counter.getValue());
                    break;
                case "09":
                    System.out.println("09: " + counter.getValue());
                    break;
                case "10":
                    System.out.println("10: " + counter.getValue());
                    break;
                case "11":
                    System.out.println("11: " + counter.getValue());
                    break;
                case "12":
                    System.out.println("12: " + counter.getValue());
                    break;
                case "13":
                    System.out.println("13: " + counter.getValue());
                    break;
                case "14":
                    System.out.println("14: " + counter.getValue());
                    break;
                case "15":
                    System.out.println("15: " + counter.getValue());
                    break;
                case "16":
                    System.out.println("16: " + counter.getValue());
                    break;
                case "17":
                    System.out.println("17: " + counter.getValue());
                    break;
                case "18":
                    System.out.println("18: " + counter.getValue());
                    break;
                case "19":
                    System.out.println("19: " + counter.getValue());
                    break;
                case "20":
                    System.out.println("20: " + counter.getValue());
                    break;
                case "21":
                    System.out.println("21: " + counter.getValue());
                    break;
                case "22":
                    System.out.println("22: " + counter.getValue());
                    break;
                case "23":
                    System.out.println("23: " + counter.getValue());
                    break;
                default:
                    System.out.println("Undefined: " + counter.getValue());
            }
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}