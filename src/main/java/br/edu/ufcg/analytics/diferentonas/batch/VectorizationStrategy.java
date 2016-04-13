package br.edu.ufcg.analytics.diferentonas.batch;

import java.util.Random;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.sql.DataFrame;

/**
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 *
 */
public enum VectorizationStrategy {

	COUNT{
		@Override
		public DataFrame extractFeatures(Configuration config, DataFrame df, String inputColumnName,
				String outputColumnName) {

			int vocabSize = config.getInt("diferentonas.similarity.count.vocabsize", 262144);
			double minDF = config.getDouble("diferentonas.similarity.count.mindf", 1.0);
			double minTF = config.getDouble("diferentonas.similarity.count.mintf", 1.0);

			return new CountVectorizer().setInputCol(inputColumnName).setOutputCol(outputColumnName)
					.setMinDF(minDF)
					.setMinTF(minTF)
					.setVocabSize(vocabSize)
					.fit(df).transform(df);
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
				String outputColumnName) {

			String featuresColumnName = "tmp_lda";
			df = COUNT.extractFeatures(config, df, inputColumnName, featuresColumnName);

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
			
			return new LDA().setFeaturesCol(featuresColumnName).setTopicDistributionCol(outputColumnName)
					.setK(k).setMaxIter(maxIter).setLearningDecay(learningDecay)
					.setLearningOffset(learningOffset).fit(df).transform(df);
		}
	};

	public abstract DataFrame extractFeatures(Configuration config, DataFrame df, String inputColumnName,
			String outputColumnName);

	public String getCriteria() {
		return this.name().toLowerCase();
	}

}
