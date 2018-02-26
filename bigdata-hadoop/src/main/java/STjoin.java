import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by JackManWu on 2018/2/24.
 */
public class STjoin {
    public static int time = 0;

    public static class Map extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String childName = new String();
            String parentName = new String();
            String relationtype = new String();
            String line = value.toString();
            int i = 0;
            while (line.charAt(i) != ' ') {
                i++;
            }
            String[] values = {line.substring(0, i), line.substring(i + 1)};
            if (values[0].compareTo("child") != 0) {
                childName = values[0];
                parentName = values[1];
                relationtype = "1";
                context.write(new Text(values[1]), new Text(relationtype + "+" + childName + "+" + parentName));
                relationtype = "2";
                context.write(new Text(values[0]), new Text(relationtype + "+" + childName + "+" + parentName));
            }
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (time == 0) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }
            int grandchildnum = 0;
            String[] grandchild = new String[10];
            int grandparentnum = 0;
            String[] grandparent = new String[10];
            Iterator ite = values.iterator();
            while (ite.hasNext()) {
                String record = ite.next().toString();
                int len = record.length();
                int i = 2;
                if (len == 0) {
                    continue;
                }
                char relationtype = record.charAt(0);
                String childname = new String();
                String parentname = new String();
                while (record.charAt(i) != '+') {
                    childname = childname + record.charAt(i);
                    i++;
                }
                i = i + 1;
                while (i < len) {
                    parentname = parentname + record.charAt(i);
                    i++;
                }
                if (relationtype == '1') {
                    grandchild[grandchildnum] = childname;
                    grandchildnum++;
                } else {
                    grandparent[grandparentnum] = parentname;
                    grandparentnum++;
                }
            }
            if (grandparentnum != 0 && grandchildnum != 0) {
                for (int i = 0; i < grandchildnum; i++) {
                    for (int j = 0; j < grandparentnum; j++) {
                        context.write(new Text(grandchild[i]), new Text(grandparent[j]));
                    }
                }
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
        Job job = Job.getInstance(conf, "ST join");
        job.setJarByClass(STjoin.class);//要执行的jar中的类

        job.setMapperClass(STjoin.Map.class);
        job.setCombinerClass(STjoin.Reduce.class);
        job.setReducerClass(STjoin.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/home/wujinlei/work/stjoin/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/home/wujinlei/work/stjoin/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
