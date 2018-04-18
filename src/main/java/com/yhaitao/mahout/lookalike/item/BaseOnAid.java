package com.yhaitao.mahout.lookalike.item;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 * 基于广告的协同过滤推荐。
 * @author yhaitao
 *
 */
public class BaseOnAid {
	public static void main(String[] args) throws IOException, TasteException {
		// String modelFile = "D:/yanghaitao/lookalike/tmp/item/train.txt";
		// String trainFile = "D:/yanghaitao/lookalike/preliminary_contest_data/train.csv";
		
		// 模型数据
		// CSVUtils.readTrainDataTo(trainFile, modelFile);
		
		// 
		DataModel model = new FileDataModel(new File(args[0]));

		RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
			@Override
			public Recommender buildRecommender(DataModel model) throws TasteException {
				ItemSimilarity similarity = new PearsonCorrelationSimilarity(model);
				return new GenericItemBasedRecommender(model, similarity);
			}
		};
		
		//获取推荐结果
		List<RecommendedItem> recommendations = recommenderBuilder.buildRecommender(model).recommend(48708770, 4);
		for (RecommendedItem recommendation : recommendations) {
		    System.out.println(recommendation);
		}
	}
}
