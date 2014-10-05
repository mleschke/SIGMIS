package workshopone;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException
    {
        int wordCount = 0;
        
        for (IntWritable value : values)
        {
            wordCount += value.get();
        }


        // Write total wordcount 
        context.write(key, new IntWritable(wordCount));
    }
}
