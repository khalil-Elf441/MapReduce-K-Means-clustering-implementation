import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeans
{
    final public static String PROP_BARY_PATH = "bary";
    final public static int ITER_MAX = 1; //on commence avec 1 pour les tests

    public static List<BaryWritable> readBarycenters(Configuration conf, String filename) throws IOException
    {
        Path path = new Path(conf.get(PROP_BARY_PATH) + "/" + filename);
        Reader reader = new Reader(conf, Reader.file(path));

        List<BaryWritable> res = new ArrayList<>();
        IntWritable key = new IntWritable();
        BaryWritable val = new BaryWritable();

        while(reader.next(key, val))
        {
            res.add(val);
            key = new IntWritable();
        	val = new BaryWritable();
        }
        reader.close();

        return res;
    }

    public static void recordBarycenters(Configuration conf, String filename, List<BaryWritable> barycenters) throws IOException
    {
        Path path = new Path(conf.get(PROP_BARY_PATH) + "/" + filename);
        FileSystem fs = FileSystem.get(conf);
        
        if(fs.exists(path))
            fs.delete(path, true);

        Writer writer = SequenceFile.createWriter(
            conf,
            Writer.file(path),
            Writer.keyClass(IntWritable.class),
            Writer.valueClass(BaryWritable.class)
        );

        for(BaryWritable b : barycenters)
            writer.append(new IntWritable(b.getClusterId()), b);

        writer.close();
    }

    public static void initBarycenters(String fin, Configuration config, int k) throws IOException
    {
        Path path = new Path(fin);
    	FileSystem fs = FileSystem.get(config);
    	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    	List<BaryWritable> barycenters = new ArrayList<>();

        int i = 0;
        while(i < k)
        {
            String line = br.readLine();
    		if(line.charAt(0) == 'a')
    			continue;
    		PointWritable p = new PointWritable(line);
    		barycenters.add(new BaryWritable(i, p.getCoordinates()));
            ++i;
        }

    	KMeans.recordBarycenters(config, "all", barycenters);
    }

    public static void updateBarycenters(Configuration config) throws IOException
    {
    	Path path = new Path(config.get(PROP_BARY_PATH) + "/reducer");
    	FileSystem fs = FileSystem.get(config);
    	RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
    	List<BaryWritable> barycentres = new ArrayList<>();
        LocatedFileStatus status = null;
        List<BaryWritable> barycentresLus = null;
    	while(it.hasNext())
        {
    		status = it.next();
    		barycentresLus = readBarycenters(config, "/reducer/" + status.getPath().getName());
    		barycentres.addAll(barycentresLus);
    		fs.delete(status.getPath(), false);
    	}
    	recordBarycenters(config, "all", barycentres);
    }

    public static Job setupJob(int iteration, String fin, String fout) throws IOException
    {
        Configuration conf = new Configuration();
    	Path baryPath = new Path("tmp/barycenters");
		conf.set(PROP_BARY_PATH, baryPath.toString());
		Job job = new Job(conf, "KMeans program");
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setOutputKeyClass(BaryWritable.class);
		job.setOutputValueClass(PointWritable.class);
		
		Path inputFilePath = new Path(fin);
		FileInputFormat.addInputPath(job, inputFilePath);
		
		Path outputFilePath = new Path(fout);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputFilePath))
			fs.delete(outputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
    	
    	return job;
    }

    public static class KMeansMapper extends Mapper<LongWritable, Text, BaryWritable, PointWritable>
    {
        private List<BaryWritable> barycenters;

        @Override
        public void setup(Mapper<LongWritable, Text, BaryWritable, PointWritable>.Context context) throws IOException, InterruptedException
        {
            this.barycenters = KMeans.readBarycenters(context.getConfiguration(), "all");
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // on ignore l'en-tête du fichier
            if(value.toString().charAt(0) == 'a')
                return;

            PointWritable newPoint = new PointWritable(value.toString());
            BaryWritable res = null;
            double shortestDis = 9999.0;

            for(BaryWritable b : this.barycenters)
            {
                double distance = newPoint.computeDistance(b);
                if(distance < shortestDis)
                {
                    shortestDis = distance;
                    res = b;
                }
            }

            context.write(res, newPoint);
        }
    }

    public static class KMeansReducer extends Reducer<BaryWritable, PointWritable, BaryWritable, PointWritable>
    {
        private List<BaryWritable> barycenters = new ArrayList<>();

        @Override
        protected void cleanup(Reducer<BaryWritable, PointWritable, BaryWritable, PointWritable>.Context context) throws IOException, InterruptedException
        {
            int taskId = context.getTaskAttemptID().getTaskID().getId();
        	String filename = "reducer/task" + taskId;
        	KMeans.recordBarycenters(context.getConfiguration(), filename, barycenters);
        }

        @Override
        public void reduce(BaryWritable key, Iterable<PointWritable> val, Context context) throws IOException, InterruptedException
        {
            double[] curCluster = new double[key.getCoordinates().length];
        	BaryWritable newBarycenter = new BaryWritable(key.getClusterId(), curCluster);
        	int count = 0;

        	for(PointWritable p : val)
            {
        		newBarycenter.add(p);
        		++count;
            	context.write(key, p);
        	}

        	newBarycenter.divideBy(count);
        	this.barycenters.add(newBarycenter);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        final int K = Integer.parseInt(args[0]);
		final String input = args[1];
		final String output = args[2];
		Job job = null;

		for(int i = 0; i < ITER_MAX; ++i)
        {
			job = setupJob(i, input, output);
            // on initialise uniquement lors de la première itération
			if(i == 0)
				initBarycenters(input, job.getConfiguration(), K);
            if(!job.waitForCompletion(true))
                System.exit(1);
			updateBarycenters((job.getConfiguration()));
		}

        System.exit(0);
    }
}
