/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene.examples;

import java.io.File;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.result.TupleResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.lucene.LuceneSail;
import org.openrdf.sail.lucene.LuceneSailSchema;
import org.openrdf.sail.memory.MemoryStore;


/**
 * Example code showing how to use the LuceneSail
 * @author sauermann
 */
public class LuceneSailExample {

	/**
	 * Create a lucene sail and use it
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		createSimple();		

	}
	
	/**
	 * Create a LuceneSail and add some triples to it, ask a query. 
	 */
	public static void createSimple() throws Exception {
		// create a sesame memory sail
		MemoryStore memoryStore = new MemoryStore();

		// create a lucenesail to wrap the memorystore
		LuceneSail lucenesail = new LuceneSail();		
		// set this parameter to let the lucene index store its data in ram
		lucenesail.setParameter(LuceneSail.LUCENE_RAMDIR_KEY, "true");
		// set this parameter to store the lucene index on disk
		// lucenesail.setParameter(LuceneSail.LUCENE_DIR_KEY, "./data/mydirectory");
		
		// wrap memorystore in a lucenesail
		lucenesail.setDelegate(memoryStore);
		
		// create a Repository to access the sails
		SailRepository repository = new SailRepository(lucenesail);
		repository.initialize();
		
		// add some test data, the FOAF ont
		SailRepositoryConnection connection = repository.getConnection();
		connection.begin();
		try {
//			connection.setAutoCommit(false);
			File file = new File("/Users/sschenk/Downloads/foaf.rdfs");
			System.out.println(file.exists());
			connection.add(
					file,
					"", 
					RDFFormat.RDFXML);
			connection.commit();
			
			// search for all resources that mention "person"
			String queryString = "PREFIX search:   <"+LuceneSailSchema.NAMESPACE+"> \n" +
					"SELECT ?x ?score ?snippet WHERE {?x search:matches ?match. \n" +
					"?match search:query \"person\"; \n" +
					"search:score ?score; \n" +
					"search:snippet ?snippet. }" ;
			System.out.println("Running query: \n"+queryString);
			TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			TupleResult result = query.evaluate();
			try { 
				// print the results
				while (result.hasNext()){
					BindingSet bindings = result.next();
					System.out.println("found match: ");
					for (Binding binding : bindings) {
						System.out.println(" "+binding.getName()+": "+binding.getValue());
					}
				}
			} finally {
				result.close();
			}
		} finally {
			connection.close();
			repository.shutDown();
		}
		
	}
}
