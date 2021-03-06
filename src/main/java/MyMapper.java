import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author zhengxt
 */
public class MyMapper extends Mapper<LongWritable,Text,Text, IntWritable> {
    /**
     * map方法，首先你要先知道输入是什么，你要什么样的输出。
     * 本例子每次输入一行以逗号分隔的字符串，需要统计单词的个数，因此输出应该是拆分后的(value,1)
     * @param key hdfs中key默认是LongWritable
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private  Text word = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString(),",");
        while (st.hasMoreTokens()) {
            word.set(st.nextToken());
            context.write(word,new IntWritable(1));
        }
    }
}
