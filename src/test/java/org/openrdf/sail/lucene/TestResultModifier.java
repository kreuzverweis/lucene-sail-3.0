/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;


import static org.openrdf.sail.lucene.LuceneSailSchema.MATCHES;
import static org.openrdf.sail.lucene.LuceneSailSchema.QUERY;
import static org.openrdf.sail.lucene.LuceneSailSchema.SCORE;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;

import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.result.TupleResult;
import org.openrdf.sail.memory.MemoryStore;
import org.openrdf.store.StoreException;


/**
 *
 * @author sschenk
 */
public class TestResultModifier extends TestCase {

	public static final String LIMIT_QUERY_STRING;

	public static final String ORDER_QUERY_STRING;

	public static final URI SUBJECT_1 = new URIImpl("urn:subject1");

	public static final URI SUBJECT_2 = new URIImpl("urn:subject2");

	public static final URI SUBJECT_3 = new URIImpl("urn:subject3");

	public static final URI SUBJECT_4 = new URIImpl("urn:subject4");

	public static final URI SUBJECT_5 = new URIImpl("urn:subject5");

	public static final URI CONTEXT_1 = new URIImpl("urn:context1");

	public static final URI CONTEXT_2 = new URIImpl("urn:context2");

	public static final URI CONTEXT_3 = new URIImpl("urn:context3");

	public static final URI PREDICATE_1 = new URIImpl("urn:predicate1");

	public static final URI PREDICATE_2 = new URIImpl("urn:predicate2");

	public static final URI PREDICATE_3 = new URIImpl("urn:predicate3");

	protected LuceneSail sail;

	protected Repository repository;

	protected RepositoryConnection connection;

	static {
		StringBuilder buffer = new StringBuilder();
		buffer.append("SELECT Subject, Score ");
		buffer.append("FROM {Subject} <" + MATCHES + "> {} ");
		buffer.append(" <" + QUERY + "> {Query}; ");
		buffer.append(" <" + SCORE + "> {Score} ");
		buffer.append(" LIMIT 2 ");
		LIMIT_QUERY_STRING = buffer.toString();
		buffer = new StringBuilder();
		buffer.append("SELECT ?Subject ?Score ");
		buffer.append("WHERE { ?Subject <" + MATCHES + "> [ ");
		buffer.append(" <" + QUERY + "> ?Query; ");
		buffer.append(" <" + SCORE + "> ?Score ]}");
		buffer.append(" ORDER BY ?Subject ");
		ORDER_QUERY_STRING = buffer.toString();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp()
	throws IOException, StoreException
	{
		// set logging, uncomment this to get better logging for debugging
		// org.apache.log4j.BasicConfigurator.configure();
		// TODO: disable logging for org.openrdf.query.parser.serql.SeRQLParser, which is not possible
		// to confogure using just the Logger

		// setup a LuceneSail
		LuceneIndex index = new LuceneIndex(new RAMDirectory(), new StandardAnalyzer(Version.LUCENE_CURRENT));
		MemoryStore memoryStore = new MemoryStore();
		// enable lock tracking
		info.aduna.concurrent.locks.Properties.setLockTrackingEnabled(true);
		sail = new LuceneSail();
		sail.setDelegate(memoryStore);
		sail.setLuceneIndex(index);

		// create a Repository wrapping the LuceneSail
		repository = new SailRepository(sail);
		repository.initialize();

		// add some statements to it
		connection = repository.getConnection();
		connection.begin();
		connection.add(SUBJECT_1, PREDICATE_1, connection.getValueFactory().createLiteral("one"));
		connection.add(SUBJECT_1, PREDICATE_1, connection.getValueFactory().createLiteral("five"));
		connection.add(SUBJECT_1, PREDICATE_2, connection.getValueFactory().createLiteral("two"));
		connection.add(SUBJECT_2, PREDICATE_1, connection.getValueFactory().createLiteral("one"));
		connection.add(SUBJECT_2, PREDICATE_2, connection.getValueFactory().createLiteral("three"));
		connection.add(SUBJECT_3, PREDICATE_1, connection.getValueFactory().createLiteral("four"));
		connection.add(SUBJECT_3, PREDICATE_2, connection.getValueFactory().createLiteral("one"));
		connection.add(SUBJECT_3, PREDICATE_3, SUBJECT_1);
		connection.add(SUBJECT_3, PREDICATE_3, SUBJECT_2);
		connection.commit();	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown()
	throws StoreException
	{
		connection.close();
		repository.shutDown();
	}

	public void testSimpleLimitQuery()
	throws StoreException, MalformedQueryException
	{
		// fire a query for all subjects with a given term
		TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SERQL, LIMIT_QUERY_STRING);
		query.setBinding("Query", new LiteralImpl("one"));
		TupleResult result = query.evaluate();

		int results = 0;
		while(result.next() != null) {
			results++;
		}

		assertTrue(results == 2);

	}

	public void testOrder() throws StoreException, MalformedQueryException {
		// fire a query for all subjects with a given term
		TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, ORDER_QUERY_STRING);
		query.setBinding("Query", connection.getValueFactory().createLiteral("one"));
		TupleResult result = query.evaluate();

		assertTrue(SUBJECT_1.equals(result.next().getBinding("Subject").getValue()));
		assertTrue(SUBJECT_2.equals(result.next().getBinding("Subject").getValue()));
		assertTrue(SUBJECT_3.equals(result.next().getBinding("Subject").getValue()));
		assertFalse(result.hasNext());
		}
	
	
}
