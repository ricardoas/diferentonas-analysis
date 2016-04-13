package br.edu.ufcg.analytics.diferentonas.batch;

import java.io.Serializable;

public class SimilarityResult implements Serializable, Comparable<SimilarityResult> {

	private static final long serialVersionUID = 2329463754939889864L;
	private int id;
	private int similarId;
	private double score;

	public SimilarityResult(int id, int similarId, double score) {
		super();
		this.id = id;
		this.similarId = similarId;
		this.score = score;
	}

	public SimilarityResult() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the id
	 */
	public long getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the similarId
	 */
	public int getSimilarId() {
		return similarId;
	}

	/**
	 * @param similarId
	 *            the similarId to set
	 */
	public void setSimilarId(int similarId) {
		this.similarId = similarId;
	}

	/**
	 * @return the score
	 */
	public double getScore() {
		return score;
	}

	/**
	 * @param score
	 *            the score to set
	 */
	public void setScore(double score) {
		this.score = score;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SimilarityResult [id=" + id + ", similarId=" + similarId + ", score=" + score + "]";
	}

	@Override
	public int compareTo(SimilarityResult o) {
		return Double.compare(o.score, this.score);
	}

}
