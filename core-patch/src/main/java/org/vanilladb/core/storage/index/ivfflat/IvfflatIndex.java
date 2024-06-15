/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.index.ivfflat;

import static java.sql.Types.NULL;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

// import java.util.HashMap;
// import java.util.LinkedList;
// import java.util.List;
// import java.util.Map;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
// import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.VectorType;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
// import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

/**
 * A static hash implementation of {@link Index}. A fixed number of buckets is
 * allocated, and each bucket is implemented as a file of index records.
 */
public class IvfflatIndex extends Index {
	private static Logger logger = Logger.getLogger(IvfflatIndex.class.getName());

	/**
	 * Field names of the schema of index records.
	 */
	public static final String SCHEMA_KEY = "i_emb";
	public static final String SCHEMA_ID = "i_id";
	public static final String SCHEMA_RID_BLOCK = "block";
	public static final String SCHEMA_RID_ID = "id";

	// Number of buckets for the index
	public static final int NUM_BUCKETS;

	// Static initializer to set the number of buckets from properties
	static {
		NUM_BUCKETS = CoreProperties.getLoader().getPropertyAsInteger(
				IvfflatIndex.class.getName() + ".NUM_BUCKETS", 100);
	}

	/**
	 * Calculates the search cost for the index.
	 *
	 * @param keyType   the type of the search key
	 * @param totRecs   total number of records
	 * @param matchRecs number of matching records
	 * @return the estimated search cost
	 */
	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
		int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
		int index_rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(indexSchema(keyType));
		if (NUM_BUCKETS == 0)
			return 0;
		return (totRecs / rpb) / NUM_BUCKETS + NUM_BUCKETS / index_rpb;
	}

	/**
	 * Returns the schema of the index records.
	 *
	 * @param keyType the type of the search key
	 * @return the schema of the index records
	 */
	public static Schema schema(SearchKeyType keyType) {
		Schema sch = new Schema();
		sch.addField(SCHEMA_KEY, keyType.get(0));
		sch.addField(SCHEMA_RID_BLOCK, BIGINT);
		sch.addField(SCHEMA_RID_ID, INTEGER);
		return sch;
	}

	/**
	 * Returns the schema of the index records.
	 *
	 * @param keyType the type of the search key
	 * @return the schema of the index records
	 */
	public static Schema indexSchema(SearchKeyType keyType) {
		Schema sch = new Schema();
		sch.addField(SCHEMA_KEY, keyType.get(0));
		sch.addField(SCHEMA_ID, INTEGER);
		return sch;
	}

	private SearchKey searchKey;
	private RecordFile indexRecordFile = null;
	private RecordFile rf;
	private boolean isBeforeFirsted;
	private String loggerTblName;
	private int loggerIndex;
	private static VectorConstant[] vectorIndexes = new VectorConstant[NUM_BUCKETS];
	private static boolean hasLoaded = false;
	private float[] maxVector;
	private VectorConstant maxVectorCons;

	/**
	 * Opens a hash index for the specified index.
	 *
	 * @param ii       the information of this index
	 * @param keyType  the type of the search key
	 * @param tx       the calling transaction
	 */
	public IvfflatIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		super(ii, keyType, tx);
		if (keyType.length() != 1)
			throw new UnsupportedOperationException();

		String tblName = ii.indexName();
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);

		// Initialize the maximum vector
		VectorType vt = (VectorType) keyType.get(0);
		maxVector = new float[vt.getArgument()];
		for (int i = 0; i < vt.getArgument(); i++) {
			maxVector[i] = Float.MAX_VALUE;
		}
		maxVectorCons = new VectorConstant(maxVector);

		if (ti != null) {
			this.indexRecordFile = ti.open(tx, false);
			preLoadToMemory();
		}

		if (ti == null) {
			VanillaDb.catalogMgr().createTable(tblName, indexSchema(keyType), tx);
			ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
			this.indexRecordFile = ti.open(tx, false);
			indexRecordFile.beforeFirst();
			logger.info("ivfvlat initializing...");
			initializeIndex();
		}
	}

	/**
	 * Preloads the index data into memory.
	 */
	@Override
	public void preLoadToMemory() {
		if (hasLoaded)
			return;

		indexRecordFile.beforeFirst();
		Integer indexNum = 0;

		// Initialize the vector indexes with maximum vectors
		for (int i = 0; i < NUM_BUCKETS; i++) {
			vectorIndexes[i] = maxVectorCons;
		}

		// Load vectors from index file into memory
		while (indexRecordFile.next()) {
			VectorConstant v = (VectorConstant) indexRecordFile.getVal(SCHEMA_KEY);
			indexNum = (Integer) indexRecordFile.getVal(SCHEMA_ID).asJavaVal();
			vectorIndexes[indexNum] = v;
		}
		indexRecordFile.close();
		hasLoaded = true;
	}

	/**
	 * Positions the index before the first index record having the specified
	 * search key. The method hashes the search key to determine the bucket, and
	 * then opens a {@link RecordFile} on the file corresponding to the bucket.
	 * The record file for the previous bucket (if any) is closed.
	 *
	 * @see Index#beforeFirst(SearchRange)
	 */
	@Override
	public void beforeFirst(SearchRange searchRange) {
		if (!hasLoaded)
			throw new IllegalStateException("You must call preLoadToMemory() before iterating index '"
					+ ii.indexName() + "'");

		close();

		// Support equality query only
		if (!searchRange.isSingleValue())
			throw new UnsupportedOperationException();

		this.searchKey = searchRange.asSearchKey();

		// Calculate the distance using Euclidean function and find the closest bucket
		EuclideanFn edf = new EuclideanFn(SCHEMA_KEY);
		edf.setQueryVector((VectorConstant) this.searchKey.get(0));
		double minDist = Double.MAX_VALUE;
		Integer minIndex = 0;

		// Find the closest vector
		for (Integer indexNum = 0; indexNum < NUM_BUCKETS; indexNum++) {
			VectorConstant v = vectorIndexes[indexNum];
			double dist = edf.distance(v);
			if (dist < minDist) {
				minDist = dist;
				minIndex = indexNum;
			}
		}

		// Open the corresponding bucket file
		String tblName = ii.indexName() + "-" + minIndex;
		loggerTblName = tblName;
		loggerIndex = minIndex;
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
		if (ti == null) {
			VanillaDb.catalogMgr().createTable(tblName, schema(keyType), tx);
			ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
			this.rf = ti.open(tx, false);
		} else {
			this.rf = ti.open(tx, false);
		}
		rf.beforeFirst();
		isBeforeFirsted = true;
	}

	/**
	 * Moves to the next index record having the search key.
	 *
	 * @see Index#next()
	 */
	@Override
	public boolean next() {
		if (!isBeforeFirsted)
			throw new IllegalStateException("You must call beforeFirst() before iterating index: '" + ii.indexName() + "'");

		while (rf.next())
			if (getKey().equals(searchKey))
				return true;
		return false;
	}

	/**
	 * Retrieves the data record ID from the current index record.
	 *
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
		int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
		return new RecordId(new BlockId(dataFileName, blkNum), id);
	}

	/**
	 * Inserts a new index record into this index.
	 *
	 * @see Index#insert(SearchKey, RecordId, boolean)
	 */
	@Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		if (!hasLoaded)
			return;

		// Position before the first index record for the given search key
		beforeFirst(new SearchRange(key));

		// Log the logical operation start
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();

		// Insert the data into the index
		rf.insert();
		rf.setVal(SCHEMA_KEY, key.get(0));
		rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block().number()));
		rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
		rf.close();

		// Log the logical operation end
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
	}

	/**
	 * Deletes the specified index record.
	 *
	 * @see Index#delete(SearchKey, RecordId, boolean)
	 */
	@Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		if (!hasLoaded)
			return;

		// Position before the first index record for the given search key
		beforeFirst(new SearchRange(key));

		// Log the logical operation start
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();

		// Delete the specified entry
		while (next())
			if (getDataRecordId().equals(dataRecordId)) {
				rf.delete();
				return;
			}

		// Log the logical operation end
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
	}

	/**
	 * Closes the index by closing the current table scan.
	 *
	 * @see Index#close()
	 */
	@Override
	public void close() {
		if (rf != null)
			rf.close();
	}

	/**
	 * Returns the size of the file.
	 *
	 * @param fileName the name of the file
	 * @return the size of the file
	 */
	private long fileSize(String fileName) {
		tx.concurrencyMgr().readFile(fileName);
		return VanillaDb.fileMgr().size(fileName);
	}

	/**
	 * Retrieves the search key from the current index record.
	 *
	 * @return the search key
	 */
	private SearchKey getKey() {
		Constant[] vals = new Constant[keyType.length()];
		vals[0] = rf.getVal(SCHEMA_KEY);
		return new SearchKey(vals);
	}

	/**
	 * Initializes the IVF flat index by inserting default vectors into each bucket.
	 */
	public void initializeIndex() {
		VectorType vt = (VectorType) keyType.get(0);
		for (int i = 0; i < NUM_BUCKETS; i++) {
			indexRecordFile.insert();
			indexRecordFile.setVal(SCHEMA_ID, new IntegerConstant(i));
			indexRecordFile.setVal(SCHEMA_KEY, new VectorConstant(vt.getArgument()));
		}
	}
	
	public void TrainIndex(IvfflatIndex ivfIdx) {
		// reset index
		close();
		// train index
		IvfflatIndexTrainer ivfflatIndexTrainer = new IvfflatIndexTrainer(ivfIdx, keyType, tx, ii, indexRecordFile, vectorIndexes, NUM_BUCKETS, maxVectorCons);
		logger.info("INFO: training the indexes.");
		ivfflatIndexTrainer.trainIndex();
	}
}
