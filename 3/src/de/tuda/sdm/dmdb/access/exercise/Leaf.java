package de.tuda.sdm.dmdb.access.exercise;

import java.util.HashMap;
import java.util.Iterator;

import de.tuda.sdm.dmdb.access.AbstractIndexElement;
import de.tuda.sdm.dmdb.access.LeafBase;
import de.tuda.sdm.dmdb.access.RowIdentifier;
import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.storage.AbstractPage;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
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
		AbstractRecord readRec = this.getUniqueBPlusTree().getTable().getPrototype().clone();
		System.out.println(rec.getValue(0));
		System.out.println(rec.getValue(1));
		System.out.println(rec.getValue(2));
		this.indexPage.read(Integer.parseInt(rec.getValue(this.getUniqueBPlusTree().SLOT_POS).toString()), readRec);
		return readRec;
	}

	@Override
	public boolean insert(T key, AbstractRecord record) {
		if (this.getIndexPage().getNumRecords() == 0) {
			this.getIndexPage().insert(record);
		} else {
			AbstractRecord rec = lookup(key);
			if (rec.getValue(UniqueBPlusTreeBase.KEY_POS).compareTo(key) == 0) {
				return false;
			} else {
				this.getIndexPage().insert(record);
				if (this.isFull()) {
					AbstractIndexElement<T> leaf1 = this.createInstance();
					AbstractIndexElement<T> leaf2 = this.createInstance();
					this.split(leaf1, leaf2);
					
				}
			}
		}
		return true;
	}

	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Leaf<T>(this.uniqueBPlusTree);
	}
}