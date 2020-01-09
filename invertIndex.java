
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class InvertIndex_origin {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text(); // 存储单词和URL组合
        private Text valueInfo = new Text(); // 存储词频
        private FileSplit split; // 存储Split对象
        // 实现map函数
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                // key值由单词和文件名组成, value 值初始化为 1. 组成key-value对:
                // 如: (MapReduce:file1.txt, 1)
                
				/**********Begin**********/
				int splitIndex = split.getPath().toString().indexOf("file");
                String filename = split.getPath().toString().substring(splitIndex);
				context.write(new Text(itr.nextToken()+":"+filename),new Text("1"));
				
				/**********End**********/
            }
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();
        // 实现reduce函数， 将相同key值的value加起来
        // 并将(单词:文件名, value) 转换为 （单词， 文件名:value）
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				/**********Begin**********/
			int count = 0;
            // 统计词频
            for(Text s1:values){
				String str = s1.toString();
				count = count + Integer.parseInt(s1.toString());
			}
			String text = key.toString();
			String[] split = text.split(":");
            // 重新设置value值由URL和词频组成
			
			
			
            // 重新设置key值为单词
			
			context.write(new Text(split[0]),new Text(split[1]+":"+count));
                /**********End**********/

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        // 实现reduce函数, 将相同单词的value聚合成一个总的value，每个value之间用`;`隔开, 最后以`;`结尾
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            	/**********Begin**********/
			String outValue = "";
			for(Text s1 : values){
				outValue = outValue+s1.toString()+";";
			}
			context.write(key,new Text(outValue));
			    /**********End**********/

        }
    }

    public static void main(String[] args) throws Exception {
        // 第一个参数为 输入文件目录路径， 第二个参数为输出结果路径
        Configuration conf = new Configuration();

        if (args.length != 2) {
            System.err.println("Usage: Inverted Index <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Inverted Index");
        job.setJarByClass(InvertIndex_origin.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

