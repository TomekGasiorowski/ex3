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
import de.tuda.sdm.dmdb.storage.RowPage;
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
		AbstractRecord readRec = this.getUniqueBPlusTree().getTable()
				.lookup(Integer.parseInt(rec.getValue(1).toString()), Integer.parseInt(rec.getValue(2).toString()));
		return readRec;
	}

	@Override
	public boolean insert(T key, AbstractRecord record) {
		System.out.println(this.getUniqueBPlusTree().getTable().lookup(2, 0));
		AbstractRecord rec = this.getUniqueBPlusTree().getLeafRecPrototype().clone();
		this.binarySearch(key, rec);
		//if (rec.getValue(UniqueBPlusTreeBase.KEY_POS).compareTo(key) == 0) {
			// replace old value
		//} 
		//else {
			RowIdentifier rowID = this.getUniqueBPlusTree().getTable().insert(record);
			AbstractRecord localRec = this.getUniqueBPlusTree().getLeafRecPrototype().clone();

			localRec.setValue(this.getUniqueBPlusTree().KEY_POS, key);
			localRec.setValue(this.getUniqueBPlusTree().PAGE_POS, new SQLInteger(rowID.getPageNumber()));
			localRec.setValue(this.getUniqueBPlusTree().SLOT_POS, new SQLInteger(rowID.getSlotNumber()));

			int i = this.getIndexPage().insert(localRec);
			if (this.isFull()) {
				AbstractIndexElement<T> leaf1 = this.createInstance();
				AbstractIndexElement<T> leaf2 = this.createInstance();
				this.split(leaf1, leaf2);
				AbstractIndexElement<T> newnode = new Node<T>(this.getUniqueBPlusTree());
				AbstractRecord r = this.getUniqueBPlusTree().getLeafRecPrototype().clone();
				leaf2.getIndexPage().read(0, r);
				newnode.getIndexPage().insert(r);
				AbstractPage ap = new RowPage(this.getUniqueBPlusTree().getNodeRecPrototype().getFixedLength());
				
			}
		//}
		return true;
	}

	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Leaf<T>(this.uniqueBPlusTree);
	}
}