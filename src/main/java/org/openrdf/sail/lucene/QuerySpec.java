/*
 * Copyright Aduna, DFKI and L3S (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

/**
 * A QuerySpec holds information extracted from a TupleExpr corresponding with a
 * single Lucene query.
 * Access the patterns or use the get-methods to get the names of the 
 * variables to bind.
 */
public class QuerySpec {

	private StatementPattern matchesPattern;

	private StatementPattern queryPattern;

	private StatementPattern propertyPattern;

	private StatementPattern scorePattern;

	private StatementPattern snippetPattern;

	private StatementPattern typePattern;

	// GEO extensions - kreuzverweis
	private StatementPattern fromPattern;
	private StatementPattern toPattern;
	private StatementPattern geoLatPattern;
	private StatementPattern geoLongPattern;
	private StatementPattern geoTolerancePattern;

	public StatementPattern getFromPattern() {
		return fromPattern;
	}

	public StatementPattern getToPattern() {
		return toPattern;
	}

	public StatementPattern getGeoLatPattern() {
		return geoLatPattern;
	}

	public StatementPattern getGeoLongPattern() {
		return geoLongPattern;
	}
	
	public StatementPattern getGeoTolerancePattern() {
		return geoTolerancePattern;
	}
	// end GEO extensions - kreuzverweis

	private Resource subject;

	private String queryString;

	private URI propertyURI;

// GEO extensions - modified - kreuzverweis
//	public QuerySpec(StatementPattern matchesPattern, StatementPattern queryPattern, StatementPattern propertyPattern,
//			StatementPattern scorePattern, StatementPattern snippetPattern, StatementPattern typePattern,
//			Resource subject, String queryString, URI propertyURI)
	public QuerySpec(StatementPattern matchesPattern, StatementPattern queryPattern, StatementPattern fromPattern, 
			StatementPattern toPattern, StatementPattern geoLatPattern, StatementPattern geoLongPattern, 
			StatementPattern geoTolerancePattern, StatementPattern propertyPattern,
			StatementPattern scorePattern, StatementPattern snippetPattern, StatementPattern typePattern,
			Resource subject, String queryString, URI propertyURI)
	{
		this.matchesPattern = matchesPattern;
		this.queryPattern = queryPattern;
		this.propertyPattern = propertyPattern;
		this.scorePattern = scorePattern;
		this.snippetPattern = snippetPattern;
		this.typePattern = typePattern;
		this.subject = subject;
		this.queryString = queryString;
		this.propertyURI = propertyURI;
		// GEO enxtensions - kreuzverweis
		this.fromPattern = fromPattern;
		this.toPattern = toPattern;
		this.geoLatPattern = geoLatPattern;
		this.geoLongPattern = geoLongPattern;
		this.geoTolerancePattern = geoTolerancePattern;
		// end GEO enxtensions - kreuzverweis
	}

	public StatementPattern getMatchesPattern() {
		return matchesPattern;
	}
	
	/**
	 * return the name of the bound variable that should match the query
	 * @return the name of the variable or null, if no name set
	 */
	public String getMatchesVariableName() {
		if (matchesPattern != null)
			return matchesPattern.getSubjectVar().getName();
		else
			return null;
	}

	public StatementPattern getQueryPattern() {
		return queryPattern;
	}
	
	public StatementPattern getPropertyPattern() {
		return propertyPattern;
	}
	
	public StatementPattern getScorePattern() {
		return scorePattern;
	}
	
	/**
	 * The variable name associated with the query score
	 * @return the name or null, if no score is queried in the pattern
	 */
	public String getScoreVariableName() {
		if (scorePattern != null)
			return scorePattern.getObjectVar().getName();
		else
			return null;
	}

	public StatementPattern getSnippetPattern() {
		return snippetPattern;
	}
	
	public String getSnippetVariableName() {
		if (snippetPattern != null)
			return snippetPattern.getObjectVar().getName();
		else
			return null;
	}

	public StatementPattern getTypePattern() {
		return typePattern;
	}
	
	/**
	 * the type of query, must equal {@link LuceneSailSchema#}.
	 * A null type is possible, but not valid.
	 * @return the type of the Query or null, if no type assigned.
	 */
	public URI getQueryType() {
		if (typePattern != null)
			return (URI)typePattern.getObjectVar().getValue();
		else
			return null;
	}

	public Resource getSubject() {
		return subject;
	}
	
	/**
	 * return the literal expression of the query or null,
	 * if none set. (null values are possible, but not valid).
	 * @return the query or null
	 */
	public String getQueryString() {
		// this should be the same as ((Literal) queryPattern.getObjectVar().getValue()).getLabel();
		return queryString;
	}
	
	/**
	 * @return The URI of the property who's literal values should be searched, or <code>null</code> 
	 */
	public URI getPropertyURI() {
		return propertyURI;
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("QuerySpec\n");
		buffer.append("   query string = \"" + queryString + "\"\n");
		buffer.append("   property uri = " + propertyURI + "\n");
		buffer.append("   subject = " + subject);
		append(matchesPattern, buffer);
		append(queryPattern, buffer);
		append(propertyPattern, buffer);
		append(scorePattern, buffer);
		append(snippetPattern, buffer);
		append(typePattern, buffer);
		return buffer.toString();
	}

	private void append(StatementPattern pattern, StringBuilder buffer) {
		if(pattern == null)
			return;
		
		buffer.append("   ");
		buffer.append("StatementPattern\n");
		append(pattern.getSubjectVar(), buffer);
		append(pattern.getPredicateVar(), buffer);
		append(pattern.getObjectVar(), buffer);
	}

	private void append(Var var, StringBuilder buffer) {
		buffer.append("      ");
		buffer.append(var.toString());
	}
}
