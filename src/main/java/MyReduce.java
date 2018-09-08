import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhengxt
 */
public class MyReduce extends Reducer<String, IntWritable,String,Integer> {
    @Override
    protected void reduce(String key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for(IntWritable i : values) {
            total += i.get();
        }
        context.write(key,total);
    }
}
