package de.tuda.sdm.dmdb.access.exercise;

import java.util.HashMap;
import java.util.Iterator;

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
	}

	@Override
	public boolean insert(T key, AbstractRecord record) {
		if (this.getIndexPage().getNumRecords() == 0) {
			this.getIndexPage().insert(record);
		} else {
			AbstractRecord rec = lookup(key);
			if (rec.getValue(UniqueBPlusTreeBase.KEY_POS).compareTo(key) == 0) {
				return false;
			} else if (this.isFull()) {
				
				int found = binarySearch(key);

				Leaf<T> leaf1 = (Leaf<T>) this.createInstance();
				Leaf<T> leaf2 = (Leaf<T>) this.createInstance();
				this.split(leaf1, leaf2);
				AbstractIndexElement<T> toParentNode = leaf2.getUniqueBPlusTree().getIndexElements().get(found);
				// pull up to the parent node

				leaf2.getUniqueBPlusTree().getIndexElements().remove(found);
				leaf2.indexPage.insert(record);
			} else {
				this.getIndexPage().insert(record);
			}
		}
		return true;
	}

	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Leaf<T>(this.uniqueBPlusTree);
	}
}