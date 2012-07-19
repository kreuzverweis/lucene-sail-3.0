/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import java.io.IOException;

import junit.framework.TestCase;

import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.result.GraphResult;
import org.openrdf.sail.memory.MemoryStore;
import org.openrdf.store.StoreException;


/**
 *
 * @author Enrico Minack
 */
public class GraphQueryTest extends TestCase {

	protected Repository repository;

	protected RepositoryConnection connection;

	@Override
	public void setUp()
		throws IOException, StoreException
	{
		// setup a memory sail
		MemoryStore sail = new MemoryStore();

		// create a Repository wrapping the sail
		repository = new SailRepository(sail);
		repository.initialize();

		connection = repository.getConnection();
	}
	
	@Override
	public void tearDown()
		throws StoreException
	{
		connection.close();
		repository.shutDown();
	}

	public void testOne() throws MalformedQueryException, StoreException {
		StringBuilder query = new StringBuilder();
		query.append("CONSTRUCT DISTINCT \n");
		query.append("    {r1} <uri:p> {r2} , \n");
		query.append("    {r1} <uri:p> {r3} \n");

		GraphQuery tq = connection.prepareGraphQuery(QueryLanguage.SERQL, query.toString());
		tq.setBinding("r1", new URIImpl("uri:one"));
		tq.setBinding("r2", new URIImpl("uri:two"));
		tq.setBinding("r3", new URIImpl("uri:three"));
		GraphResult result = tq.evaluate();
		
		int i=0;
		while(result.hasNext()) {
			Statement statement = result.next();
			System.out.println(statement);
			i++;
		}
		
		assertEquals(2, i);
	}
}
