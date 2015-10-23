/*
 * btfile.C - function members of class BTreeFile 
 * 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

// Define your error message here
const char* BtreeErrorMsgs[] = { "File Not Found", 
};

static error_string_table btree_table( BTREE, BtreeErrorMsgs);

BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
  PageId startPage;
  
  // Use MINIBASE_DB->get_file_entry method to open given file.
  returnStatus = MINIBASE_DB->get_file_entry(filename, startPage);

  // Use MINIBASE_BM->pinPage method to pin the startPage 
  returnStatus = MINIBASE_BM->pinPage(startPage, (Page* &)btpage);

}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
  
  PageId startPage;
  
  if ( OK != (returnStatus = MINIBASE_DB->get_file_entry(filename, startPage)))
  {
    returnStatus = MINIBASE_BM->newPage(startPage, (Page* &)btpage);
    returnStatus = MINIBASE_DB->add_file_entry(filename, startPage);
    returnStatus = MINIBASE_BM->pinPage(startPage, (Page* &)btpage);
  }
  
  // typecast the btpage allocated to sortedPage. 
  // Initialize the sortedPage class and set it's type to LEAF.
  // As initially its a b-tree with only one LEAF.

  ((SortedPage *)btpage)->init(startPage);
  ((SortedPage *)btpage)->set_type(LEAF);
  
  btpage->key_type = keytype;
  btpage->keysize = keysize;
  btpage->root = INVALID_PAGE;
  file = filename;

}

BTreeFile::~BTreeFile ()
{
  
  // Just unpin header index page. 
  // Don't delete the file entry as the same index can be made use to create another index.

  PageId startPage;
  Status stat;
  stat = MINIBASE_DB->get_file_entry(file, startPage);
  stat = MINIBASE_BM->unpinPage(startPage, TRUE);
}

Status BTreeFile::destroyFile ()
{
  return OK;

}

Status BTreeFile::insert(const void *key, const RID rid) {
  
  Status stat;
  RID tmpRid;
  PageId rootPage;
  PageId insertPage;
  Page *page;
  void **splitKey;
  PageId splitPage = INVALID_PAGE;
  if (btpage->key_type == attrInteger)
    splitKey = (void **)new int;
  if (btpage->key_type == attrString)
    splitKey = (void **)new char[MAX_KEY_SIZE1];
        
  *splitKey = NULL;
  // check if key is null, if so return an error.
  if (key == NULL) {
    return MINIBASE_FIRST_ERROR(BTREE, KEY_ERROR);
  }

  // Check if the btpage->root is an INVALID_PAGE, if so then need to create a new LEAF page.
  
  if(btpage->root == INVALID_PAGE)
  {
    cout << "As there is no root page, I am creating one" << endl;
    if (OK != (stat = MINIBASE_BM->newPage(rootPage, (Page *&)page)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
    
    ((SortedPage *)page)->init(rootPage);
    ((SortedPage *)page)->set_type(LEAF);
    btpage->root = rootPage;
    
    if (OK != (stat = MINIBASE_BM->unpinPage(rootPage, TRUE)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
  }
  
  insertPage = rootPage;

  if (OK != (stat = insertElement(insertPage, key, rid, splitKey, splitPage)))
    return MINIBASE_FIRST_ERROR(BTREE, INSERT_ERROR);
 
  
  if (*splitKey == NULL && splitPage == INVALID_PAGE)
      cout << "There is no splitting" << endl;
      return OK;
  
  PageId topIndex;
  if (OK != (stat = MINIBASE_BM->newPage(topIndex, (Page *&)page)))
    return MINIBASE_CHAIN_ERROR(BUFMGR, stat);

  ((SortedPage *)page)->init(topIndex);
  ((SortedPage *)page)->set_type(LEAF);
  ((BTIndexPage *)page)->setLeftLink(btpage->root);
  btpage->root = topIndex;

  if (OK != (stat = ((BTIndexPage *)page)->insertKey(splitKey, btpage->key_type, splitPage, tmpRid)))
      return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);
  
  if (OK != (stat = MINIBASE_BM->unpinPage(topIndex, TRUE)))
      return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
  
  cout << "Index Page is " << topIndex << endl;
  return OK;
}



Status BTreeFile::ifLeafPage( PageId pageNo, 
                              const void *key, 
                              const RID rid, 
                              void **splitKey, 
                              PageId& splitPage)

{
  Status stat;
  Page *page;
  RID curRid;
  PageId splitPageNo;
  int pivot;
  int numRec;
  int count = 1;

  if (OK != (stat = MINIBASE_BM->pinPage(pageNo, (Page *&)page)))
      return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
  
  BTLeafPage *leafPage = (BTLeafPage *)page;
  
  if (OK == (stat = leafPage->insertRec(key, btpage->key_type, rid, curRid)))
  {
    
    if (OK != (stat = MINIBASE_BM->unpinPage(pageNo, TRUE)))
      return MINIBASE_CHAIN_ERROR(BUFMGR, stat);

    *splitKey = NULL;
    splitPage = INVALID_PAGE;
    return OK;
  }

  else
  {
    // There is no space left to insert record in this leaf page.
    // Need to split leaf page and insert.
    // 1. Create a new LEAF page.
    // 2. Find d = numberOfrecords()/2, round it off to even. 
    // 3. Copy d+1 to n records to newLeafPage.
    // 4. Delete d+1 entries from currentLeafPage.
    // 5. Insert current key to respective LEAF page.
    // 6. Take the first key of new LEAF page as splitkey and newleaf pageid as split page.
    //
    

    if (OK != (stat = MINIBASE_BM->newPage(splitPageNo, (Page *&)page)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
    
    BTLeafPage *splitLeafPage = (BTLeafPage *)page;
    splitLeafPage->init(splitPageNo);
  
    numRec = leafPage->numberOfRecords();
    pivot = numRec/2;
    
    RID entryRid;
    RID tmpRid;
    RID dataRid;
    RID splitRid;
    void *entryKey;
    cout << "Splitting the page " << endl;
    
    if (OK != (stat = leafPage->get_first(entryRid, entryKey, dataRid)))
        return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
    
    //cout << "First Data in Leaf Page is " << *(int *)entryKey << endl;
    //cout << "Pivot value is " << pivot << endl;
    //cout << "Number of Entries is " << numRec << endl;
    while (count <= pivot)
    {
      //cout << "value of count is " << count << endl;
      if (OK != (stat = leafPage->get_next(entryRid, entryKey, dataRid)))
        if (stat == DONE) 
            return OK;
        else 
          return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
      
      //cout << "Getting key in scan " << *(int *)entryKey << endl;
      ++count;
    }
    //cout << "Seek done" << endl; 
     
    int start = 0;
    while(count < numRec)
    {
      //cout << " value of count is " << count << endl;
      if (start == 0) {
      if (OK != (stat = leafPage->get_next(entryRid, entryKey, dataRid)))
        if (stat != DONE)
          return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
      }
      //cout << "Getting key in insertScan " << *(int *)entryKey << endl;
      ++count;
      // start inserting records to splitLeafPage.
      if (OK != (stat = splitLeafPage->insertRec(entryKey, btpage->key_type, dataRid, tmpRid)))
          return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
      
      RID copyRid;
      copyRid = entryRid;
      if (OK != (stat = leafPage->get_next(entryRid, entryKey, dataRid))){
          if (stat == DONE) {
              stat = OK;
          }
          else {
             return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
          }
      }
      if (OK != (stat = leafPage->deleteKey(copyRid)))
          return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
      start = 1;
      entryRid = copyRid;
      
    }
    
    //cout << "Inserting done" << endl;
    // Now compare the key with first record of newLeafPage.

    if (OK != (stat = splitLeafPage->get_first(splitRid, entryKey, entryRid)))
        return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
    
   // cout << "First Key in splitLeafPage is " << *(int *)entryKey << endl;
    if (keyCompare(key, entryKey, btpage->key_type) < 0) 
    {
      // Insert in leafPage.
      //cout << "key is " << *(int *)key << "entryKey is " << *(int *)entryKey << endl;
      if (OK != (stat = leafPage->insertRec(key, btpage->key_type,rid, tmpRid)))
          MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
    }
    else 
    {
      if (OK != (stat = splitLeafPage->insertRec(key, btpage->key_type,rid, tmpRid)))
          MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
    }
    //cout << "Setting link nodes " << endl;

    splitLeafPage->setPrevPage(pageNo);
    splitLeafPage->setNextPage(leafPage->getNextPage());
    leafPage->setNextPage(splitPageNo);
    
    // Get first record of splitLeafPage.

    if (OK != (stat = splitLeafPage->get_first(splitRid, entryKey, entryRid)))
      return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, stat);
    //cout << "First key in splitleaf page is " << *(int *)entryKey << endl;

    *splitKey = entryKey;
    splitPage = splitPageNo;
    
    //cout << "Current page number is " << pageNo << endl;
    //cout << "Split page number is " << splitPageNo << endl;
    //cout << "splitKey is " << *(int *)(*splitKey) << endl;
    // Unpin LeafPage and SplitLeafPage 
    
    if (OK != (stat = MINIBASE_BM->unpinPage(pageNo, TRUE)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);

    if (OK != (stat = MINIBASE_BM->unpinPage(splitPageNo, TRUE)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
    
    return OK;
  }

}

    
Status BTreeFile::insertElement(PageId pageNo, 
                                const void *key, 
                                const RID rid,
                                void **splitKey,
                                PageId& splitPage) 
{
  
    Status stat;
    Page *page;
    PageId splitPageNo;
    PageId entryPage;
    void *entryKey;
    RID entryRid;
    BTIndexPage *splitIndexPage;
    int numRec;
    int pivot;
    int count;
    // Pin the page with page number pageNo.
    //
    cout << "I am being called for page number " << pageNo << endl;
    if(OK != (stat = MINIBASE_BM->pinPage(pageNo, (Page *&)page)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
    
    SortedPage *curPage = (SortedPage *)page;
    
    if ( curPage->get_type() == LEAF) 
    {
      cout << "I am a leaf page, so i will just insert key,rec" << endl;
      if(OK != (stat = ifLeafPage(pageNo, key, rid, splitKey, splitPage)))
          return MINIBASE_FIRST_ERROR(BTREE, IFLEAF_ERROR);
      cout << "eVerything done in ifLeafPage " << endl;
      return OK;
    }

    if (curPage->get_type() == INDEX) 
    {
      // Given current page is INDEX, so we need to find LEAF page number to insert record.
      BTIndexPage *pageIndex = (BTIndexPage *)curPage;
      PageId targetPage;

      if (OK != (stat = pageIndex->get_page_no(key, btpage->key_type, targetPage)))
          MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);
      
      // Call insertElement again to Insert this key, record in specified
      
      if (OK != (stat = insertElement(targetPage, key, rid, splitKey, splitPage)))
          MINIBASE_FIRST_ERROR(BTREE, INSERT_ERROR);
      
      cout << "splitKey and splitpage is " << *(int *)(*splitKey) << "and" << splitPage << endl;
      if (*splitKey == NULL && splitPage == INVALID_PAGE)
      {
        return OK;
      }

      // There has been a split, so insert that split key and page to this index page.

      if (OK != (stat = MINIBASE_BM->pinPage(pageNo, (Page *&)page)))
          return MINIBASE_CHAIN_ERROR(BUFMGR, stat);

      pageIndex = (BTIndexPage *)curPage;

      if (OK == (stat = pageIndex->insertKey(splitKey, btpage->key_type, splitPage, entryRid)))
      {
        splitKey = NULL;
        splitPage = INVALID_PAGE;
        return OK;
      }
      else 
      {
        if (OK != (stat = MINIBASE_BM->newPage(splitPageNo, (Page *&)page)))
          return MINIBASE_CHAIN_ERROR(BUFMGR, stat);

        splitIndexPage = (BTIndexPage *)page;
        splitIndexPage->init(splitPageNo);

        numRec = pageIndex->numberOfRecords();
        pivot = numRec/2;

        
        if (OK != (stat = pageIndex->get_first(entryRid, entryKey, entryPage)))
          return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);
        
        while (count < pivot)
        {
          if (OK != (stat = pageIndex->get_next(entryRid, entryKey, entryPage)))
            return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);

          ++count;
        }

        if (OK != (stat = pageIndex->get_next(entryRid, entryKey, entryPage)))
            return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);

        while(count < numRec)
        {
          ++count;

          if (OK != (stat = splitIndexPage->insertKey(entryKey, btpage->key_type, entryPage, entryRid)))
              return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);

          RID copyRid = entryRid;
          
          if (OK != (stat = pageIndex->get_next(entryRid, entryKey, entryPage)))
            return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);
          
          if (OK != (stat = pageIndex->deleteKey(entryKey, btpage->key_type, copyRid)))
              return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);
        }
      }
      

      // Now find a spot for the current splitkey in either of the two Index pages.
      
      if (OK != (stat = splitIndexPage->get_first(entryRid, entryKey, entryPage)))
          return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);

      if (keyCompare(splitKey, entryKey, btpage->key_type) < 0) 
      {
        if (OK != (stat = pageIndex->insertKey(splitKey, btpage->key_type, splitPage, entryRid)))
            return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);
      }
      else
      {
        if (OK != (stat = splitIndexPage->insertKey(splitKey, btpage->key_type, splitPage, entryRid)))
            return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);
      }

      if (OK != (stat = splitIndexPage->get_first(entryRid, entryKey, entryPage)))
          return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);
      
      splitIndexPage->setLeftLink(entryPage);

      if (OK != (stat = splitIndexPage->deleteKey(entryKey, btpage->key_type, entryRid)))
          return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, stat);

      *splitKey = entryKey;
      splitPage = splitPageNo;

      if (OK != (stat = MINIBASE_BM->unpinPage(pageNo)))
          return MINIBASE_CHAIN_ERROR(BUFMGR, stat);

      if (OK != (stat = MINIBASE_BM->unpinPage(splitPageNo)))
          return MINIBASE_CHAIN_ERROR(BUFMGR, stat);

      return OK;
    }

    if ( curPage->get_type() == LEAF) 
    {
      if(OK != (stat = ifLeafPage(pageNo, key, rid, splitKey, splitPage)))
          return MINIBASE_FIRST_ERROR(BTREE, IFLEAF_ERROR);
    }
    
}

Status BTreeFile::Delete(const void *key, const RID rid) {
  // put your code here
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  // put your code here
}
