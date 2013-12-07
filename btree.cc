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
      //if (key<testkey || key==testkey) {
      if (key<testkey) {
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
  SIZE_T offset;
  KEY_T keyhold;
  BTreeNode root;
        
  rc =  Lookup(key, (VALUE_T&)value);
  if(rc!=ERROR_NONEXISTENT){
    return ERROR_CONFLICT;
  }

  //load root
  rc = root.Unserialize(buffercache, superblock.info.rootnode);
  if (rc) {  return rc; }

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

  if(newnode!=0){
    //check if full
    //if not, just insert
    if(root.info.numkeys<root.info.GetNumSlotsAsInterior()){
      //get where to insert key
      int insertAt; //save offset where key is inserted so you know where to put value
      int found = 0;
      for (offset=0;offset<root.info.numkeys;offset++) { 
        rc=root.GetKey(offset,keyhold);
        if (rc) {  return rc; }
        if (newkey<keyhold) {
          insertAt=offset;
          found=1;
          break;
        }
      }
      if(found==0){
        insertAt = root.info.numkeys;
      }

      //END CASE



      //slide everything over
      int i;
      SIZE_T ptrhold;
      root.info.numkeys++;
      for(i=root.info.numkeys; i>insertAt+1; i--){
        rc=root.GetKey(i-2,keyhold);
        if (rc) {  return rc; }
        rc=root.SetKey(i-1,keyhold);
        if (rc) {  return rc; }
        
        rc=root.GetPtr(i-2,ptrhold);
        if (rc) {  return rc; }
        rc=root.SetPtr(i-1,ptrhold);
        if (rc) {  return rc; }
      }





      //insert the new stuff
      rc=root.SetKey(insertAt,newkey);
      if (rc) {  return rc; }
      rc=root.SetPtr(insertAt+1,newnode);
      if (rc) {  return rc; }

      //increment number of keys
      //root.info.numkeys++;
      newnode=0;

      return root.Serialize(buffercache, superblock.info.rootnode);
    }
  
    //else, split
    else{
      rc = Split(superblock.info.rootnode, key, value, newnode, newkey);
      if (rc) {  return rc; }

      //make new root
      SIZE_T newrootptr;
      BTreeNode newroot;

      rc = AllocateNode(newrootptr);
      if (rc) {  return rc; }
      newroot = root;

      //set key ptrs
      newroot.SetKey(0, newkey);
      newroot.SetPtr(0, superblock.info.rootnode);
      newroot.SetPtr(1, newnode);

      //reset count
      newroot.info.numkeys = 1;

      newroot.info.nodetype=BTREE_ROOT_NODE;

      //set superblock root
      superblock.info.rootnode = newrootptr;

      //write to disk
      return newroot.Serialize(buffercache, newrootptr);

    }
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::InsertInternal(SIZE_T &node, const KEY_T &key, const VALUE_T &value, SIZE_T &newnode, KEY_T &newkey)
{
  BTreeNode b;
  BTreeNode child;
  BTreeNode child2;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T keyhold;
  SIZE_T childptr;
  SIZE_T childptr2;
  int found; //if insert location has been found
  int childfound;

  //load node
  rc= b.Unserialize(buffercache,node);
  if (rc!=ERROR_NOERROR) { return rc;}

  //figure out which child it should go to
  //the desired child will be saved in childptr
  // Scan through key/ptr pairs
  childfound=0;
  for (offset=0;offset<b.info.numkeys;offset++) { 
    rc=b.GetKey(offset,keyhold);
    if (rc) {  return rc; }
    if (key<keyhold) {
      // OK, so we now have the first key that's larger
      // so we need to recurse on the ptr immediately previous to 
      // this one, if it exists
      rc=b.GetPtr(offset,childptr);
      if (rc) { return rc; }
      childfound=1;
      break;
    }
  }
  // if we got here, we need to go to the next pointer, if it exists
  if (b.info.numkeys>0 && childfound==0) { 
    rc=b.GetPtr(b.info.numkeys,childptr);
    if (rc) { return rc; }
  } 
  if (b.info.numkeys==0) {
    // There are no keys at all on this node, so nowhere to go
    //should only get here if root on initialization
    //make TWO children 
    rc = AllocateNode(childptr);
    if (rc) {  return rc; }
    rc = AllocateNode(childptr2);
    if (rc) {  return rc; }


    child = b;
    child.info.numkeys=0;
    child.info.nodetype = BTREE_LEAF_NODE;
    rc = child.Serialize(buffercache, childptr);
    if (rc) {  return rc; }


    child2 = child;
    rc = child2.Serialize(buffercache, childptr2);
    if (rc) {  return rc; }


    b.info.numkeys=1;
    b.SetKey(0, key);
    b.SetPtr(1, childptr);
    b.SetPtr(0, childptr2);
    b.Serialize(buffercache, node);
    //return ERROR_NONEXISTENT;
  }

  //check what kind of node the child is
  //load node
  rc = child.Unserialize(buffercache,childptr);
  if (rc!=ERROR_NOERROR) { return rc;}

  //if leaf
  //cout << child.info.nodetype;
  if(child.info.nodetype==BTREE_LEAF_NODE){

    //if not full, insert
    if(child.info.numkeys<child.info.GetNumSlotsAsLeaf()){

      //get where to insert key
      int insertAt; //save offset where key is inserted so you know where to put value
      
      if(child.info.numkeys==0){
        insertAt=0;
        child.info.numkeys++;
      }
      else{
        found = 0;
        for (offset=0;offset<child.info.numkeys;offset++) { 
          rc=child.GetKey(offset,keyhold);
          if (rc) {  return rc; }
          if (key<keyhold) {
            insertAt=offset;
            found = 1;
            break;
          }
        }
        if(found==0){
          insertAt = child.info.numkeys;
        }     

        //slide everything over
        child.info.numkeys++;
        int i;
        VALUE_T valhold;
        for(i=child.info.numkeys; i>insertAt+1; i--){
          rc=child.GetKey(i-2,keyhold);
          if (rc) {  return rc; }
          rc=child.SetKey(i-1,keyhold);
          if (rc) {  return rc; }
          
          rc=child.GetVal(i-2,valhold);
          if (rc) {  return rc; }
          rc=child.SetVal(i-1,valhold);
          if (rc) {  return rc; }
        }

        // //slide everything over
        // int i;
        // VALUE_T valhold;
        // for(i=child.info.numkeys-1; i>=insertAt; i--){
        //   rc=child.GetKey(i,keyhold);
        //   if (rc) {  return rc; }
        //   rc=child.SetKey(i+1,keyhold);
        //   if (rc) {  return rc; }
          
        //   rc=child.GetVal(i,valhold);
        //   if (rc) {  return rc; }
        //   rc=child.SetVal(i+1,valhold);
        //   if (rc) {  return rc; }
        // }
      }

      //insert the new stuff
      rc=child.SetKey(insertAt,key);
      if (rc) {  return rc; }
      rc=child.SetVal(insertAt,value);
      if (rc) {  return rc; }

      //increment number of keys
      //child.info.numkeys++;
      newnode=0;

      return child.Serialize(buffercache,childptr);
    }

    
    //if full, split, insert into proper child
    else{
      //call split, split will insert
      //set newnode and newkey to return
      return Split(childptr, key, value, newnode, newkey);
    }
  }

  //else, child is internal
  else{
    //recursive call
    rc = InsertInternal(childptr, key, value, newnode, newkey);
    if (rc!=ERROR_NOERROR) { return rc;}

    //if newnode was added, update keys in child
      //if the update makes it full now, split, rewrite newnode to be the newly added node from split

    if(newnode!=0){
      //check if full
      //if not, just insert
      if(child.info.numkeys<child.info.GetNumSlotsAsInterior()){
        //get where to insert key
        int insertAt; //save offset where key is inserted so you know where to put value
        for (offset=0;offset<child.info.numkeys;offset++) { 
          rc=child.GetKey(offset,keyhold);
          if (rc) {  return rc; }
          if (newkey<keyhold) {
            insertAt=offset;
            break;
          }
        }

        //slide everything over
        int i;
        SIZE_T ptrhold;
        for(i=child.info.numkeys-1; i>=insertAt; i--){
          rc=child.GetKey(i,keyhold);
          if (rc) {  return rc; }
          rc=child.SetKey(i+1,keyhold);
          if (rc) {  return rc; }
          
          rc=child.GetPtr(i,ptrhold);
          if (rc) {  return rc; }
          rc=child.SetPtr(i+1,ptrhold);
          if (rc) {  return rc; }
        }

        //insert the new stuff
        rc=child.SetKey(insertAt,newkey);
        if (rc) {  return rc; }
        rc=child.SetPtr(insertAt,newnode);
        if (rc) {  return rc; }

        //increment number of keys
        child.info.numkeys++;
        newnode=0;

        return child.Serialize(buffercache,childptr);
      }
      
      //else, split
      else{
        return Split(childptr, key, value, newnode, newkey);
      }
    }
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Split(SIZE_T &node_to_split, const KEY_T &key, const VALUE_T &value, SIZE_T &newnode, KEY_T &newkey)
{
  // // WRITE ME
  BTreeNode old;
  BTreeNode nnode;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T keyhold;
  VALUE_T valhold;
  SIZE_T ptrhold;
  //SIZE_T ptr;
  int counter;
  int insertAt; //holds offset of insert
  int inArray; //check if new key is in array yet 1 if yes, 0 if no
  KEY_T keyarr[old.info.numkeys+1];
  VALUE_T valarr[old.info.numkeys+1];
  SIZE_T ptrarr[old.info.numkeys+2];
  //generate nnode, copy of the node we want to split
  rc = old.Unserialize(buffercache, node_to_split);
  if (rc!=ERROR_NOERROR) { return rc;}
  nnode = old;

  

  // //two cases: leaf or not leaf
  if(old.info.nodetype==BTREE_LEAF_NODE){
    // int numkeys_old; //num keys to keep in old node
    // int numkeys_new; //num keys to put in new
    int n = old.info.numkeys; //total number of keys before insertion

    // numkeys_old = (n+3)/2; //(n+3) instead of (n/2) to account for rounding cuz we want ceiling
    // numkeys_new = n+1-numkeys_old; //total after insertion minus the keys in oldnode

    // old.info.numkeys = (n+2)/2; //(n+3) instead of (n/2) to account for rounding cuz we want ceiling
    // nnode.info.numkeys = n+1-old.info.numkeys; //total after insertion minus the keys in oldnode

    //fill a sorted array with all the keys, including new keys
    counter=0;
    inArray=0;
    for (offset=0;offset<old.info.numkeys;offset++) { 
      rc=old.GetKey(offset,keyhold);
      if (rc) {  return rc; }
      if (key<keyhold && inArray==0) {
        keyarr[counter]=key;
        valarr[counter]=value;
        inArray=1;
        insertAt=counter;
        counter++;
      }
      keyarr[counter]=keyhold;
      old.GetVal(offset, valhold);
      valarr[counter]=old.GetVal(offset, valhold);
      counter++;
    }
    if(inArray==0){
      keyarr[old.info.numkeys] = key;
      valarr[old.info.numkeys] = value;
    }

    old.info.numkeys = (n+2)/2; //(n+3) instead of (n/2) to account for rounding cuz we want ceiling
    nnode.info.numkeys = n+1-old.info.numkeys; //total after insertion minus the keys in oldnode

    unsigned int i; //our for loop increment

    //fill old node
    for(i=0; i<old.info.numkeys; i++){
      old.SetKey(i, keyarr[i]);
      old.SetVal(i, valarr[i]);
    }

    //set newkey return value
    newkey = keyarr[i];

    //fill new node
    //note that i is not reset to 0
    unsigned int j;
    for(j=0; j<nnode.info.numkeys; j++){
      nnode.SetKey(j, keyarr[i]);
      nnode.SetVal(j, valarr[i]);
      i++;
    }
  }

  //else internal
  else{
    if(old.info.nodetype==BTREE_ROOT_NODE){
      old.info.nodetype=BTREE_INTERIOR_NODE;
      nnode.info.nodetype=BTREE_INTERIOR_NODE;
    }
    int n = old.info.numkeys; //total number of keys before insertion

    old.info.numkeys = (n+1)/2; //(n+1) instead of (n) account for rounding cuz we want ceiling
    nnode.info.numkeys = n-old.info.numkeys; //total minus the keys in oldnode

    //fill a sorted array with all the keys, including new keys
    counter=0;
    inArray=0;
    for (offset=0;offset<old.info.numkeys;offset++) { 
      rc=old.GetKey(offset,keyhold);
      if (rc) {  return rc; }
      if (key<keyhold && inArray==0) {
        keyarr[counter]=key;
        inArray=1;
        insertAt=counter;
        counter++;
      }
      keyarr[counter]=keyhold;
      counter++;
    }
    if(inArray==0){
      keyarr[old.info.numkeys] = key;
    }

    //fill a sorted array with all the ptrs, including new ptr
    counter=0;
    inArray=0;
    for (offset=0;offset<old.info.numkeys+1;offset++) { 
      rc=old.GetPtr(offset,ptrhold);
      if (rc) {  return rc; }
      if (counter==insertAt+1 && inArray==0) {
        ptrarr[counter]=newnode;
        inArray=1;
        counter++;
      }
      ptrarr[counter]=ptrhold;
      counter++;
    }
    if(inArray==0){
      keyarr[old.info.numkeys+1] = newnode;
    }

    unsigned int i; //our for loop increment for keys
    unsigned int k; //our loop increment for ptrs
    //fill old node
    for(i=0; i<old.info.numkeys; i++){
      old.SetKey(i, keyarr[i]);
    }
    for(k=0; k<old.info.numkeys+1; k++){
      old.SetPtr(k, ptrarr[k]);
    }

    //set newkey return value
    newkey = keyarr[i];
    i++; // not inserting this one

    //fill new node
    //note that i,k is not reset to 0
    unsigned int j;
    for(j=0; j<nnode.info.numkeys; j++){
      nnode.SetKey(j, keyarr[i]);
      i++;
    }
    for(j=0; j<nnode.info.numkeys; j++){
      nnode.SetPtr(j, ptrarr[k]);
      i++;
    }
  }

  //write changes to disk
  old.Serialize(buffercache, node_to_split);
  if (rc!=ERROR_NOERROR) { return rc;}
  return nnode.Serialize(buffercache, newnode);
}
 
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, (VALUE_T&)value);

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
  rc = DisplayInternal(superblock.info.rootnode,o,display_type);
  if (rc) { return rc; }
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




