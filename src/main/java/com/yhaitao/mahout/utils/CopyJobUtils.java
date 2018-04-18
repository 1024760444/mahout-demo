package com.yhaitao.mahout.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.term.TFPartialVectorReducer;

@SuppressWarnings("deprecation")
public class CopyJobUtils {
	public static void makePartialVectors(Path input, Configuration baseConf,
			int maxNGramSize, Path dictionaryFilePath, Path output,
			int dimension, boolean sequentialAccess, boolean namedVectors,
			int numReducers)
			throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration(baseConf);
		// this conf parameter needs to be set enable serialisation of conf
		// values
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");
		conf.setInt(PartialVectorMerger.DIMENSION, dimension);
		conf.setBoolean(PartialVectorMerger.SEQUENTIAL_ACCESS,
				sequentialAccess);
		conf.setBoolean(PartialVectorMerger.NAMED_VECTOR, namedVectors);
		conf.setInt(DictionaryVectorizer.MAX_NGRAMS, maxNGramSize);
		DistributedCache.addCacheFile(dictionaryFilePath.toUri(), conf);

		Job job = new Job(conf);
		job.setJobName(
				"DictionaryVectorizer::MakePartialVectors: input-folder: "
						+ input + ", dictionary-file: " + dictionaryFilePath);
		job.setJarByClass(DictionaryVectorizer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		FileInputFormat.setInputPaths(job, input);

		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(Mapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setReducerClass(TFPartialVectorReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(numReducers);

		HadoopUtil.delete(conf, output);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}
}
