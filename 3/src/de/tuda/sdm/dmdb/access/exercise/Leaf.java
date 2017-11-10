package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.access.AbstractIndexElement;
import de.tuda.sdm.dmdb.access.LeafBase;
import de.tuda.sdm.dmdb.access.RowIdentifier;
import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.SQLVarchar;

/**
 * Index leaf
 * 
 * @author cbinnig
 */
public class Leaf<T extends AbstractSQLValue> extends LeafBase<T> {

	/**
	 * Leaf constructor
	 * 
	 * @param uniqueBPlusTree
	 *            TODO
	 */
	public Leaf(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
		super(uniqueBPlusTree);
	}

	@Override
	public AbstractRecord lookup(T key) {
		AbstractRecord rec = this.getUniqueBPlusTree().getLeafRecPrototype().clone();
		this.binarySearch(key, rec);
		return rec;
		// test commit
	}

	@Override
	public boolean insert(T key, AbstractRecord record) {
		int keynr = this.getUniqueBPlusTree().getKeyColumnNumber();
		Leaf<T> leaf1 = null;
		Leaf<T> leaf2 = null;
		AbstractSQLValue val = record.getValue(keynr);
		if (val instanceof SQLInteger) {
			leaf1 = new Leaf(new UniqueBPlusTree<SQLInteger>(this.getUniqueBPlusTree().getTable(),
					this.getUniqueBPlusTree().getKeyColumnNumber(), this.getUniqueBPlusTree().getMaxFillGrade()));
			leaf2 = new Leaf(new UniqueBPlusTree<SQLInteger>(this.getUniqueBPlusTree().getTable(),
					this.getUniqueBPlusTree().getKeyColumnNumber(), this.getUniqueBPlusTree().getMaxFillGrade()));
		}
		else {
			leaf1 = new Leaf(new UniqueBPlusTree<SQLVarchar>(this.getUniqueBPlusTree().getTable(),
					this.getUniqueBPlusTree().getKeyColumnNumber(), this.getUniqueBPlusTree().getMaxFillGrade()));
			leaf2 = new Leaf(new UniqueBPlusTree<SQLVarchar>(this.getUniqueBPlusTree().getTable(),
					this.getUniqueBPlusTree().getKeyColumnNumber(), this.getUniqueBPlusTree().getMaxFillGrade()));
		}
		this.split(leaf1, leaf2);
		
		return true;
	}

	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Leaf<T>(this.uniqueBPlusTree);
	}
}