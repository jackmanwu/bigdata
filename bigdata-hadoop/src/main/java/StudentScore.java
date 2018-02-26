import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by JackManWu on 2018/2/21.
 */
public class StudentScore {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println("行值:" + line);
            StringTokenizer tokenizer = new StringTokenizer(line, "\n");
            while (tokenizer.hasMoreTokens()) {
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizer.nextToken());
                String strName = tokenizerLine.nextToken();
                String strScore = tokenizerLine.nextToken();
                Text name = new Text(strName);
                int score = Integer.parseInt(strScore);
                context.write(name, new IntWritable(score));
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                sum += iterator.next().get();
                count++;
            }
            int average = sum / count;
            context.write(key, new IntWritable(average));
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "wujinlei");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapred.jar", "E:\\JackManWu\\hadoo-ptest\\target\\hadoop-test-1.0-SNAPSHOT.jar");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        Job job = Job.getInstance(conf, "student_score");
        job.setJarByClass(StudentScore.class);//要执行的jar中的类

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/home/wujinlei/work/student/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/home/wujinlei/work/student/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
