import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by JackManWu on 2018/2/23.
 */
public class DataSort {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSort.class);

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static IntWritable data = new IntWritable(0);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            LOGGER.info("current data: {}", data.get());
            context.write(data, new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable lineNum = new IntWritable(1);

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                IntWritable intWritable = iterator.next();
                LOGGER.info("key: {},values: {}", key, intWritable.get());
                context.write(lineNum, intWritable);
                lineNum = new IntWritable(lineNum.get() + 1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "wujinlei");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapred.jar", "E:\\JackManWu\\hadoo-ptest\\target\\hadoop-test-1.0-SNAPSHOT.jar");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        Job job = Job.getInstance(conf, "Data DataSort");
        job.setJarByClass(DataSort.class);//要执行的jar中的类

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/home/wujinlei/work/sort/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/home/wujinlei/work/sort/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
