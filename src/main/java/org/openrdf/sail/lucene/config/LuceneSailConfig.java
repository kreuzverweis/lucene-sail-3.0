/*
 * Copyright Aduna, DFKI and L3S (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene.config;

import static org.openrdf.sail.lucene.config.LuceneSailConfigSchema.INDEX_DIR;

import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.util.ModelException;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.sail.config.DelegatingSailImplConfigBase;
import org.openrdf.sail.config.SailImplConfig;
import org.openrdf.store.StoreConfigException;

public class LuceneSailConfig extends DelegatingSailImplConfigBase {

	/*-----------*
	 * Variables *
	 *-----------*/

	private String indexDir;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public LuceneSailConfig() {
		super(LuceneSailFactory.SAIL_TYPE);
	}

	public LuceneSailConfig(SailImplConfig delegate) {
		super(LuceneSailFactory.SAIL_TYPE, delegate);
	}

	public LuceneSailConfig(String luceneDir) {
		super(LuceneSailFactory.SAIL_TYPE);
		setIndexDir(luceneDir);
	}

	public LuceneSailConfig(String luceneDir, SailImplConfig delegate) {
		super(LuceneSailFactory.SAIL_TYPE, delegate);
		setIndexDir(luceneDir);
	}

	/*---------*
	 * Methods *
	 *---------*/

	public String getIndexDir() {
		return indexDir;
	}

	public void setIndexDir(String luceneDir) {
		this.indexDir = luceneDir;
	}

	@Override
	public Resource export(Model graph) {
		Resource implNode = super.export(graph);

		if (indexDir != null) {
			graph.add(implNode, INDEX_DIR, new LiteralImpl(indexDir));
		}

		return implNode;
	}

	@Override
	public void parse(Model graph, Resource implNode) throws StoreConfigException
	{
		super.parse(graph, implNode);

		try {
			Literal indexDirLit = ModelUtil.getOptionalObjectLiteral(graph, implNode, INDEX_DIR);
			if (indexDirLit != null) {
				setIndexDir(indexDirLit.getLabel());
			}
		}
		catch (ModelException e) {
			throw new StoreConfigException(e.getMessage(), e);
		}
	}
}
