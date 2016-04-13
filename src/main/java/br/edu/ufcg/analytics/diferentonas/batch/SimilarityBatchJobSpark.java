package br.edu.ufcg.analytics.diferentonas.batch;


import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

/**
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public class SimilarityBatchJobSpark extends AbstractSparkBatchJob {

	protected static final String FEATURES_COLUMN_NAME = "features";
	protected static final String STEMS_COLUMN_NAME = "stems";
	/**
	 * 
	 */
	private static final long serialVersionUID = 5129861149501988496L;
	private transient Configuration configuration;

	public SimilarityBatchJobSpark(Configuration configuration) {
		super(configuration);
		this.configuration = configuration;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run(String... args) {

		VectorizationStrategy vectorizer = VectorizationStrategy.valueOf(args[0].toUpperCase());
		SimilarityParameter parameter = SimilarityParameter.valueOf(args[1].toUpperCase());
		
		double similarityThreshold = Double.valueOf(args[2]);
		
		String keyColumnName = parameter.getKeyColumnName();
		String valueColumnName = parameter.getValueColumnName();
		
		StructType schema = new StructType(
				new StructField[] { new StructField(keyColumnName, DataTypes.IntegerType, false, Metadata.empty()),
						new StructField(valueColumnName, DataTypes.StringType, false, Metadata.empty()) });

		JavaRDD<Row> rows = javaFunctions(getJavaSparkContext())
				.cassandraTable(getKeyspace(), parameter.getInputTableName())
				.select(keyColumnName, valueColumnName).filter( row -> !row.isNullAt(valueColumnName) ).distinct()
				.map(row -> RowFactory.create(row.getInt(keyColumnName), row.getString(valueColumnName)));

		DataFrame df = getSQLContext().createDataFrame(rows, schema);

		Integer maxID = df.select(keyColumnName).toJavaRDD().map(row -> row.getInt(0)).max(new IDComparator());

		Tokenizer tokenizer = new Tokenizer().setInputCol(valueColumnName).setOutputCol(STEMS_COLUMN_NAME);

		df = tokenizer.transform(df);

		df = vectorizer.extractFeatures(configuration, df, STEMS_COLUMN_NAME, FEATURES_COLUMN_NAME);

		JavaPairRDD<Integer, List<Tuple2<Integer, Double>>> pairs = df.select(keyColumnName, FEATURES_COLUMN_NAME)
				.toJavaRDD().flatMapToPair(row -> {
					int id = row.getInt(0) - 1;
					Vector features = row.getAs(1);
					LinkedList<Tuple2<Integer, List<Tuple2<Integer, Double>>>> linkedList = new LinkedList<>();

					features.foreachActive(new AbstractFunction2<Object, Object, BoxedUnit>() {
						public BoxedUnit apply(Object t1, Object t2) {
							Integer term = (Integer) t1;
							Double termWeight = (Double) t2;
							linkedList.add(new Tuple2<>(term, Arrays.asList(new Tuple2<>(id, termWeight))));
							return BoxedUnit.UNIT;
						}
					});

					return linkedList;
				});

		JavaRDD<Vector> vectors = pairs
				.reduceByKey((v1, v2) -> Stream.concat(v1.stream(), v2.stream()).collect(Collectors.toList()))
				.map(pair -> Vectors.sparse(maxID, pair._2).toDense());

		CoordinateMatrix similarityMatrix = new RowMatrix(vectors.rdd()).columnSimilarities(similarityThreshold);

		// format output
		JavaRDD<SimilarityResult> results = similarityMatrix.entries().toJavaRDD()
				.flatMap(entry -> Arrays.asList(
						new SimilarityResult((int) entry.i() + 1, (int) entry.j() + 1, entry.value()),
						new SimilarityResult((int) entry.j() + 1, (int) entry.i() + 1, entry.value())));

		CassandraJavaUtil.javaFunctions(results)
				.writerBuilder(getKeyspace(), parameter.getOutputTableName() + vectorizer.getCriteria(), mapToRow(SimilarityResult.class)).saveToCassandra();
	}
}
