package br.edu.ufcg.analytics.diferentonas.batch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import scala.Tuple2;

/**
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 *
 */
public enum VectorizationStrategy {

	COUNT{
		@Override
		public DataFrame extractFeatures(Configuration config, DataFrame df, String inputColumnName,
				String outputColumnName) throws IOException {

			int vocabSize = config.getInt("diferentonas.similarity.count.vocabsize", 262144);
			double minDF = config.getDouble("diferentonas.similarity.count.mindf", 1.0);
			double minTF = config.getDouble("diferentonas.similarity.count.mintf", 1.0);

			CountVectorizerModel model = new CountVectorizer().setInputCol(inputColumnName).setOutputCol(outputColumnName)
					.setMinDF(minDF)
					.setMinTF(minTF)
					.setVocabSize(vocabSize)
					.fit(df);
			
			String[] vocabulary = model.vocabulary();
			try(FileWriter writer = new FileWriter(new File("vocabulario.csv"));){
				writer.write("index,stem\n");
				for (int i = 0; i < vocabulary.length; i++) {
					writer.write(String.format("%d,%s\n", i, vocabulary[i]));
				}
			}
			return model.transform(df);
		}
	},
	TFIDF{
		@Override
		public DataFrame extractFeatures(Configuration config, DataFrame df, String inputColumnName,
				String outputColumnName) {

			String rawFeaturesColumn = "rawFeatures";
			int numFeatures = config.getInt("diferentonas.similarity.tfidf.numfeatures", 262144);
			int minDocFreq = config.getInt("diferentonas.similarity.tfidf.mindocfreq", 0);
			HashingTF hashingTF = new HashingTF().setInputCol(inputColumnName).setOutputCol(rawFeaturesColumn)
					.setNumFeatures(numFeatures);
			DataFrame featurizedData = hashingTF.transform(df);
			return new IDF().setInputCol(rawFeaturesColumn).setOutputCol(outputColumnName).setMinDocFreq(minDocFreq)
					.fit(featurizedData).transform(featurizedData);
		}
	},
	WORD2VEC{
		@Override
		public DataFrame extractFeatures(Configuration config, DataFrame df, String inputColumnName,
				String outputColumnName) {
			int vectorSize = config.getInt("diferentonas.similarity.word2vec.vectorsize", 100);
			int windowSize = config.getInt("diferentonas.similarity.word2vec.windowsize", 5);
			int minCount = config.getInt("diferentonas.similarity.word2vec.mincount", 5);
			int numPartitions = config.getInt("diferentonas.similarity.word2vec.numpartitions", 1);
			int maxIterations = config.getInt("diferentonas.similarity.word2vec.maxiter", 1);
			int seed = config.getInt("diferentonas.similarity.word2vec.seed", new Random().nextInt());
			double stepSize = config.getDouble("diferentonas.similarity.word2vec.stepSize", 0.025);

			return new Word2Vec().setInputCol(inputColumnName).setOutputCol(outputColumnName).setVectorSize(vectorSize)
					.setMinCount(minCount).setMaxIter(maxIterations).setNumPartitions(numPartitions).setSeed(seed)
					.setStepSize(stepSize).setWindowSize(windowSize).fit(df).transform(df);
		}
	},
	LDA{
		@Override
		public DataFrame extractFeatures(Configuration config, DataFrame df, String inputColumnName,
				String outputColumnName) throws IOException {

			int vocabSize = config.getInt("diferentonas.similarity.count.vocabsize", 262144);
			double minDF = config.getDouble("diferentonas.similarity.count.mindf", 1.0);
			double minTF = config.getDouble("diferentonas.similarity.count.mintf", 1.0);
			String featuresColumnName = "tmp_count";

			CountVectorizerModel cvModel = new CountVectorizer().setInputCol(inputColumnName).setOutputCol(featuresColumnName)
					.setMinDF(minDF)
					.setMinTF(minTF)
					.setVocabSize(vocabSize)
					.fit(df);
			
			String[] vocabulary = cvModel.vocabulary();
			

			
			df = cvModel.transform(df);
			
			df.show(false);

			int k = config.getInt("diferentonas.similarity.lda.k", 10);
			int maxIter = config.getInt("diferentonas.similarity.lda.maxiter", 20);
			double learningDecay = config.getDouble("diferentonas.similarity.lda.learningdecay", 0.51);
			double learningOffset = config.getDouble("diferentonas.similarity.lda.learningoffset", 1024.0);

//			for (int k = 10; k < 100; k +=10) {
//				
//				org.apache.spark.ml.clustering.LDA lda = new LDA().setFeaturesCol(featuresColumnName).setTopicDistributionCol(outputColumnName)
//						.setK(k).setMaxIter(maxIter).setLearningDecay(learningDecay)
//						.setLearningOffset(learningOffset);
//				LDAModel fit = lda
//						.fit(df);
//				System.out.printf("\n\n\n perplexity %d %.4f %.4f     \n\n\n", k, fit.logLikelihood(df), fit.logPerplexity(df));
//			}
			
			LDAModel ldaModel = new LDA().setFeaturesCol(featuresColumnName).setTopicDistributionCol(outputColumnName)
					.setK(k).setMaxIter(maxIter).setLearningDecay(learningDecay)
					.setLearningOffset(learningOffset).fit(df);
			
			try(FileWriter writer = new FileWriter(new File("params.csv"));){
				writer.write("k,loglikelihood,logperplexity\n");
				writer.write(String.format("%d,%f,%f\n", k, ldaModel.logLikelihood(df), ldaModel.logPerplexity(df)));
			}

			System.out.println("  >>> VectorizationStrategy.extractFeatures()");
//			System.out.printf("\n\n\n perplexity %d %.4f %.4f     \n\n\n", k, ldaModel.logLikelihood(df), ldaModel.logPerplexity(df));
			DataFrame topics = ldaModel.describeTopics();
			
			
			List<Row> rows = topics.select("topic","termIndices", "termWeights").javaRDD().collect();
			
			
			
//			List<List<Tuple2<Integer, String>>> map2 = topics.select("topic","termIndices").javaRDD()
//					.map(row -> row.getList(1))
//					.map(l -> l.stream().map(obj -> new Tuple2<>((Integer) obj, vocabulary[(Integer) obj]))
//							.collect(Collectors.toList())).collect();
			
			
			try(FileWriter topicIndexWriter = new FileWriter(new File("topic_index.csv"));
					FileWriter topicWriter = new FileWriter(new File("topic_term.csv"));
					FileWriter topicWeightWriter = new FileWriter(new File("topic_weight.csv"));
					){
				topicIndexWriter.write("id,i_0,i_1,i_2,i_3,i_4,i_5,i_6,i_7,i_8,i_9\n");
				topicWriter.write("id,i_0,i_1,i_2,i_3,i_4,i_5,i_6,i_7,i_8,i_9\n");
				topicWeightWriter.write("id,i_0,i_1,i_2,i_3,i_4,i_5,i_6,i_7,i_8,i_9\n");
				for (Row row : rows) {
					List<Tuple2<Integer, String>> tupleList = row.getList(1).stream().map(obj -> new Tuple2<>((Integer) obj, vocabulary[(Integer) obj])).collect(Collectors.toList());
					String string = Arrays.toString(tupleList.stream().mapToInt(tuple -> tuple._1).toArray());
					String stringT = Arrays.toString(tupleList.stream().map(tuple -> tuple._2).toArray());
					topicIndexWriter.write(String.format("%d,%s,\n", row.getInt(0), string.substring(1, string.length()-1)));
					topicWriter.write(String.format("%d,%s,\n", row.getInt(0), stringT.substring(1, stringT.length()-1)));
					
					List<Double> weights = row.getList(2).stream().map(obj -> (Double) obj).collect(Collectors.toList());
					String stringW = weights.toString();
					topicWeightWriter.write(String.format("%d,%s,\n", row.getInt(0), stringW.substring(1, stringW.length()-1)));
				}
				
				
//				for (List<Tuple2<Integer, String>> tupleList : map2) {
//					String string = Arrays.toString(tupleList.stream().mapToInt(tuple -> tuple._1).toArray());
//					String stringT = Arrays.toString(tupleList.stream().map(tuple -> tuple._2).toArray());
//					topicIndexWriter.write(String.format("%d,%s,\n", tupleList.size(), string.substring(1, string.length()-1)));
//					topicWriter.write(String.format("%d,%s,\n", tupleList.size(), stringT.substring(1, stringT.length()-1)));
//				}
			}
			topics.show(false);
			System.out.println();
			System.out.println();
			System.out.println("  <<< VectorizationStrategy.extractFeatures()");
			ldaModel.save(String.format("lda_model_%d_%d", k, maxIter));
//			new LDA()
			DataFrame resultDataFrame = ldaModel.transform(df);
			rows = resultDataFrame.select("nr_convenio","features").javaRDD().collect();
			try (FileWriter resultWriter = new FileWriter(new File("result.csv"));
					) {
				for (Row row : rows) {
					String stringW = Arrays.toString(((DenseVector)row.get(1)).values()).toString();
					resultWriter.write(
							String.format("%d,%s,\n", row.getInt(0), stringW.substring(1, stringW.length() - 1)));
				}
			}
			
			return resultDataFrame;
		}
	};

	public abstract DataFrame extractFeatures(Configuration config, DataFrame df, String inputColumnName,
			String outputColumnName) throws IOException;

	public String getCriteria() {
		return this.name().toLowerCase();
	}

}
