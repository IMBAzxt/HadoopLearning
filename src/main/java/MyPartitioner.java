import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author ThinkPad
 */
public class MyPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int i) {
        if(key.toString().startsWith("a")){
            return 0%i;
        } else if(key.toString().startsWith("b")){
            return 1%i;
        } else {
            return 2%i;
        }
    }
}
