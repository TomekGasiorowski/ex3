package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.access.AbstractIndexElement;
import de.tuda.sdm.dmdb.access.exercise.Node;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.SQLInteger;

/**
 * Unique B+-Tree implementation 
 * @author cbinnig
 *
 * @param <T>
 */
public class UniqueBPlusTree<T extends AbstractSQLValue> extends UniqueBPlusTreeBase<T> {
	
	/**
	 * Constructor of B+-Tree with user-defined fil-grade
	 * @param table Table to be indexed
	 * @param keyColumnNumber Number of unique column which should be indexed
	 * @param fillGrade fill grade of index
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber, int fillGrade) {
		super(table, keyColumnNumber, fillGrade);
	} 
	
	/**
	 * Constructor for B+-tree with default fill grade
	 * @param table table to be indexed 
	 * @param keyNumber Number of unique column which should be indexed
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber) {
		this(table, keyColumnNumber, DEFAULT_FILL_GRADE);
	}	
	
	@SuppressWarnings({ "unchecked" })
	@Override
	public boolean insert(AbstractRecord record) {
		//insert record
		T key = (T) record.getValue(this.keyColumnNumber);
		if (this.getRoot() instanceof Leaf){
			return this.getRoot().insert(key, record);
		}
		else {
			return this.getRoot().insert(key, record);
		}
	}
	
	@Override
	public AbstractRecord lookup(T key) {
		if (this.getRoot() instanceof Leaf){
			return this.getRoot().lookup(key);
		}
		else {
			int sep = this.getRoot().binarySearch(key);
			return this.indexElements.get(sep).lookup(key);
		}
	}
}
