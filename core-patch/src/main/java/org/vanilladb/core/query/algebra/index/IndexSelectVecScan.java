/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
package org.vanilladb.core.query.algebra.index;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Int8VectorConstant;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.index.ivf.IVFIndex;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.query.algebra.MultiTableScan;
/**
 * The scan class corresponding to the select relational algebra operator.
 */
public class IndexSelectVecScan implements UpdateScan {
	private IVFIndex idx;
	private TableScan ts;
	//private SearchRange searchRange;
	private Int8VectorConstant vec;
	// private TableScan selected_ts;
	private MultiTableScan selected_ts;
	// private MultiTableScan selected_tss;
	/**
	 * Creates an index select scan for the specified index and search range.
	 * 
	 * @param idx
	 *            the index
	 * @param searchRange
	 *            the range of search keys
	 * @param ts
	 *            the table scan of data table
	 */
	public IndexSelectVecScan(Index idx, Int8VectorConstant vec){
		this.idx = (IVFIndex)idx;
		this.vec = vec;
		// selected_ts 是多個聚類對應的Table和在一起的TableScan
		selected_ts = this.idx.OpenTopk(vec);

	}

	/**
	 * Positions the scan before the first record, which in this case means
	 * positioning the index before the first instance of the selection
	 * constant.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		selected_ts.beforeFirst();
	}

	/**
	 * Moves to the next record, which in this case means moving the index to
	 * the next record satisfying the selection constant, and returning false if
	 * there are no more such index records. If there is a next record, the
	 * method moves the tablescan to the corresponding data record.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		return selected_ts.next();
	}

	/**
	 * Closes the scan by closing the index and the tablescan.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		idx.close();
		selected_ts.close();
	}

	/**
	 * Returns the value of the field of the current data record.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		return selected_ts.getVal(fldName);
	}

	/**
	 * Returns whether the data record has the specified field.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return selected_ts.hasField(fldName);
	}

	@Override
	public void setVal(String fldName, Constant val) {
		selected_ts.setVal(fldName, val);
	}

	@Override
	public void delete() {
		selected_ts.delete();
	}

	@Override
	public void insert() {
		selected_ts.insert();
	}

	@Override
	public RecordId getRecordId() {
		return selected_ts.getRecordId();
	}

	@Override
	public void moveToRecordId(RecordId rid) {
		selected_ts.moveToRecordId(rid);
	}
}
