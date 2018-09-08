import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zhengxt
 */
public class MapReduceTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(MapReduceTest.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReduce.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(Integer.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
