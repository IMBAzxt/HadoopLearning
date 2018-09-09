import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 学习，运行方法：将jar包放在hadoop集群的机器上执行hadoop jar HadoopLearning-1.0-SNAPSHOT.jar MapReduceTest /data/input/test /data/input/1 2
 * @author zhengxt
 */
public class MapReduceTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(MapReduceTest.class);
        job.setMapperClass(MyMapper.class);
        //设置自定义分区方法。
        job.setPartitionerClass(MyPartitioner.class);
        job.setCombinerClass(MyReduce.class);
        job.setReducerClass(MyReduce.class);
        //设置最大的reduce数。
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
