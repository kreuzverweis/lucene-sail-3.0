/*
 * Copyright Aduna, DFKI and L3S (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import org.apache.lucene.document.Document;

/**
 * Interface defining a listener for Lucene search results.
 */
public interface SearchResultListener {

	/**
	 * Called once for every search result.
	 *
	 * @param doc
	 *        The document that matched the query.
	 * @param score
	 *        The score of the matched document.
	 */
	public void handleResult(Document doc, float score);
}
