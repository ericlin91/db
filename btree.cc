#include <assert.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
      	if (op==BTREE_OP_LOOKUP) { 
      	  return b.GetVal(offset,value);
      	} else { 
      	  // BTREE_OP_UPDATE
      	  // WRITE ME
      	  rc=b.SetVal(offset, value);
      	  if(rc) { return rc; }
      	  return b.Serialize(buffercache, node);
      	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  BTreeNode b;
  ERROR_T rc;
  SIZE_T newnode;
  KEY_T newkey;

  //allocate the newnode holder
  rc = AllocateNode(newnode);
  if(rc!=ERROR_NOERROR){return rc;}

  //begin recursive call
  rc = InsertInternal(superblock.info.rootnode, key, value, newnode, newkey);
  if(rc!=ERROR_NOERROR){return rc;}

  //if newnode has something in it
    //we need to add the key ptr pair newkey/newnode
      //check if we need to split
        //if yes, split, add, make new root
        //else just add
}

ERROR_T BTreeIndex::InsertInternal(SIZE_T &node, const KEY_T &key, const VALUE_T &value, SIZE_T &newnode, KEY_T &newkey, aDDNEWPOINTER)
{
  BTreeNode b;
  BTreeNode child;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T keyhold;
  SIZE_T ptr;

  //load node
  rc= b.Unserialize(buffercache,node);
  if (rc!=ERROR_NOERROR) { return rc;}

  //figure out which child it should go to
  //the desired child will be saved in ptr
  // Scan through key/ptr pairs
  for (offset=0;offset<b.info.numkeys;offset++) { 
    rc=b.GetKey(offset,testkey);
    if (rc) {  return rc; }
    if (key<testkey) {
      // OK, so we now have the first key that's larger
      // so we need to recurse on the ptr immediately previous to 
      // this one, if it exists
      rc=b.GetPtr(offset,ptr);
      if (rc) { return rc; }
    }
  }
  // if we got here, we need to go to the next pointer, if it exists
  if (b.info.numkeys>0) { 
    rc=b.GetPtr(b.info.numkeys,ptr);
    if (rc) { return rc; }
  } 
  else {
    // There are no keys at all on this node, so nowhere to go
    return ERROR_NONEXISTENT;
  }

  //check what kind of node the child is
  //load node
  rc = child.Unserialize(buffercache,ptr);
  if (rc!=ERROR_NOERROR) { return rc;}

  //if leaf
  if(child.info.nodetype==BTREE_LEAF_NODE){

    //if not full, insert
    if(child.info.numkeys<child.info.GetNumSlotsAsLeaf()){

      //get where to insert key
      int insertAt; //save offset where key is inserted so you know where to put value
      for (offset=0;offset<child.info.numkeys;offset++) { 
        rc=child.GetKey(offset,keyhold);
        if (rc) {  return rc; }
        if (key<keyhold) {
          insertAt=offset;
          break;
        }
      }

      //slide everything over
      int i;
      VALUE_T valhold;
      for(i=child.info.numkeys-1; i>=insertAt; i--){
        rc=child.GetKey(i,keyhold);
        if (rc) {  return rc; }
        rc=child.SetKey(i+1,keyhold);
        if (rc) {  return rc; }
        
        rc=child.GetVal(i,valhold);
        if (rc) {  return rc; }
        rc=child.SetVal(i+1,valhold);
        if (rc) {  return rc; }
      }

      //insert the new stuff
      rc=child.SetKey(insertAt,key);
      if (rc) {  return rc; }
      rc=child.SetVal(insertAt,value);
      if (rc) {  return rc; }

      //increment number of keys
      child.info.numkeys++;
      newnode=NULL;
      newkey=NULL;
    }

    
    //if full, split, insert into proper child
    else{
      //call split, split will insert
      //set newnode and newkey to return
      rc = Split(child, key, value, newnode, newkey);
      if (rc) {  return rc; }
    }
  }

  //else, it's internal
  else{
    //recursive call
    rc = InsertInternal(child, key, value, newnode, newkey);
    if (rc!=ERROR_NOERROR) { return rc;}

    //if newnode was added, update keys in child
      //if the update makes it full now, split, rewrite newnode to be the newly added node from split

    if(newnode!=NULL){
      //check if full
      //if not, just insert
      if(child.info.)

      //else, split
      else{
        key = newkey;

        rc = Split(child, key, value, newnode, newkey);
        if (rc) {  return rc; }
      }
    }
  }
}


ERROR_T BTreeIndex::Split(SIZE_T &node_to_split, const KEY_T &key, const VALUE_T &value, SIZE_T &newnode, KEY_T &newkey)
{
  // WRITE ME
  BTreeNode old;
  BTreeNode nnode;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T keyhold;
  SIZE_T ptr;
  int counter;
  bool inArray; //check if new key is in array yet

  //generate nnode, copy of the node we want to split
  rc = old.Unserialize(buffercache, node_to_split);
  if (rc!=ERROR_NOERROR) { return rc;}
  nnode = old;

  //fill a sorted array with all the keys, including new keys
  KEY_T keyarr[old.info.numkeys+1];
  //VALUE_T valarr[old.info.numkeys+2];
  counter=0;
  inArray=FALSE;
  for (offset=0;offset<old.info.numkeys;offset++) { 
    rc=old.GetKey(offset,keyhold);
    if (rc) {  return rc; }
    if (key<keyhold && inArray==FALSE) {
      keyarr[counter]=key;
      inArray=TRUE;
      counter++;
    }
    keyarr[counter]=keyhold;
    counter++;
  }

  //two cases: leaf or not leaf
  if(old.info.nodetype==BTREE_LEAF_NODE){


    //leave at original node first (n+2)/2 keys and pointers
    //remember in a leaf the link pointer is at offset 0
    counter=0;
    for (offset=0;offset<(old.info.numkeys+2)/2;offset++) { 
      rc=old.SetKey(offset,inArray[counter]);
      if (rc) {  return rc; }
      if (key<keyhold && inArray==FALSE) {
        keyarr[counter]=key;
        inArray=TRUE;
        counter++;
      }
      keyarr[counter]=keyhold;
      counter++;
    }

  }
  //else internal
  else{

  }

  //write changes to disk
  old.Serialize(buffercache, node_to_split);
  if (rc!=ERROR_NOERROR) { return rc;}
  return nnode.Serialize(buffercache, newnode);
}
 
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, (VALUE_T&)value);

  //return ERROR_UNIMPL;
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  return ERROR_UNIMPL;
}
  


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  return os;
}




