/*
 * Copyright Aduna, DFKI and L3S (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * LuceneSailSchema defines predicates that can be used for expressing a Lucene
 * query in a RDF query.
 */
public class LuceneSailSchema {

	public static final String NAMESPACE = "http://www.openrdf.org/contrib/lucenesail#";

	public static final URI LUCENE_QUERY;

	public static final URI SCORE;

	public static final URI QUERY;

	public static final URI PROPERTY;

	public static final URI SNIPPET;

	public static final URI MATCHES;
	
	public static final URI FROM;

	public static final URI TO;

	public static final URI GEOLAT;

	public static final URI GEOLONG;

	public static final URI GEOTOLERANCE;

	static {
		ValueFactory factory = new ValueFactoryImpl(); // compatible with beta4: creating a new factory
		LUCENE_QUERY = factory.createURI(NAMESPACE + "LuceneQuery");
		SCORE = factory.createURI(NAMESPACE + "score");
		QUERY = factory.createURI(NAMESPACE + "query");
		PROPERTY = factory.createURI(NAMESPACE + "property");
		SNIPPET = factory.createURI(NAMESPACE + "snippet");
		MATCHES = factory.createURI(NAMESPACE + "matches");
		
		// GEO extensions - kreuzverweis
		FROM = factory.createURI(NAMESPACE + "rangeQueryFrom");
		TO = factory.createURI(NAMESPACE + "rangeQueryTo");
		GEOLAT = factory.createURI(NAMESPACE + "geoDegreesLat");
		GEOLONG = factory.createURI(NAMESPACE + "geoDegreesLong");
		GEOTOLERANCE = factory.createURI(NAMESPACE + "geoDegreesTolerance");
	}
}
