import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * compile: javac -cp $(hbase classpath) WordCountHBase.java
 * run:     java  -cp $(hbase classpath) WordCountHBase "/input/files"
 */
public class WordCountHBase {
    public static class WordcountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                this.word.set(itr.nextToken());
                context.write(this.word, one);
            }
        }
    }

    public static class WordCountTableReducer extends TableReducer<Text,IntWritable,Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put row = new Put(key.getBytes());
            row.addColumn(Bytes.toBytes("result"), Bytes.toBytes("count"), Bytes.toBytes(String.format("%d", sum)));

            context.write(key, row);
        }
    }

    public static class WordCountCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable value = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            value.set(sum);
            context.write(key, this.value);
        }
    }

    public static void main(String[] args) throws Exception {
		Configuration conf =  HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountHBase.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

        // mapper
		job.setMapperClass(WordcountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

        // combiner
        job.setCombinerClass(WordCountCombiner.class);

        // reducer
		TableMapReduceUtil.initTableReducerJob("WC", WordCountTableReducer.class, job);
		job.setReducerClass(WordCountTableReducer.class);

		job.waitForCompletion(true);
    }
}

