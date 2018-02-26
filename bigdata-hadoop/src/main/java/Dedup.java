import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by JackManWu on 2018/2/23.
 */
public class Dedup {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text line = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            context.write(line, new Text(""));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "wujinlei");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapred.jar", "E:\\JackManWu\\hadoo-ptest\\target\\hadoop-test-1.0-SNAPSHOT.jar");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        Job job = Job.getInstance(conf, "Data Deduplication");
        job.setJarByClass(Dedup.class);//要执行的jar中的类

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/home/wujinlei/work/dedup/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/home/wujinlei/work/dedup/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
