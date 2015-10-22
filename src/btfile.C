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
const char* BtreeErrorMsgs[] = {
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
  PageId rootPage;
  PageId insertPage;
  // check if key is null, if so return an error.
  if (key == NULL) {
    return MINIBASE_FIRST_ERROR(BTREEFILE, KEY_ERROR);
  }

  // Check if the btpage->root is an INVALID_PAGE, if so then need to create a new LEAF page.
  
  if(btpage->root == INVALID_PAGE)
  {
    if (OK != (stat = MINIBASE_BM->newPage(rootPage, (page *&)page)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
    
    ((SortedPage *)page)->init(rootPage);
    ((SortedPage *)page)->set_type(LEAF);
    btpage->root = rootPage;
    
    if (OK != (stat = MINIBASE_BM->unpinPage(rootPage, TRUE)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
  }
  
  insertPage = rootPage;

  if (OK != (stat = insertElement(insertPage, key, rid)))
    return MINIBASE_FIRST_ERROR(BTREEFILE, INSERT_ERROR);

  
  // Insert into this leaf page, if the space runs out, we have to split the page and add index page.
  return OK;
}



Status BTreeFile::ifLeafPage( PageId pageNo, 
                              const void *key, 
                              const RID rid, 
                              void *splitKey, 
                              PageId& splitPage)

{
  Status stat;
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

    if (OK != (stat = leafPage->get_first(entryRid, entryKey, dataRid)))
        return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, GET_FIRST_FAILED);

    while (count <= pivot)
    {
      if (OK != (stat = leafPage->get_next(entryRid, entryKey, dataRid)))
        if (stat != DONE)
          return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, GET_NEXT_FAILED);

      ++count;
    }
    
    int start=0;
    while(count < numRec)
    {
      if (start == 0)
      {
        if (OK != (stat = leafPage->get_next(entryRid, entryKey, dataRid)))
          if (stat != DONE)
            return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, GET_NEXT_FAILED);
      }
      
      ++count;
      // start inserting records to splitLeafPage.
      if (OK != (stat = splitLeafPage->insertRec(entryKey, btpage->key_type, dataRid, tmpRid)))
          return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, INSERT_REC_FAILED);
      RID copyRid;
      copyRid = entryRid;
      if (OK != (stat = leafPage->get_next(entryRid, entryKey, dataRid)))
          if (stat != DONE)
              return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, GET_NEXT_FAILED);
      
      if (OK != (stat = leafPage->deleteKey(copyRid)))
          return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, DELETE_REC_FAILED);
      // entryRid = copyRid;
      start = 1;
      
    }

    // Now compare the key with first record of newLeafPage.

    if (OK != (stat = splitLeafPage->get_first(splitRid, entryKey, entryRid)))
        return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, GET_FIRST_FAILED);

    if (keyCompare(key, entryKey, btpage->key_type) < 0) 
    {
      // Insert in leafPage.
      if (OK != (stat = leafPage->insertRec(key, btpage->key_type,rid, tmpRid)))
          MINIBASE_CHAIN_ERROR(BTLEAFPAGE, INSERT_REC_ERROR);
    } 
    else 
    {
      if (OK != (stat = splitLeafPage->insertRec(key, btpage->key_type,rid, tmpRid)))
          MINIBASE_CHAIN_ERROR(BTLEAFPAGE, INSERT_REC_ERROR);
    }

    splitLeafPage->setPrevPage(pageNo);
    splitLeafPage->setNextPage(leafPage->getNextPage());
    leafPage->setNextPage(splitPageNo);
    
    // Get first record of splitLeafPage.

    if (OK != (stat = splitLeafPage->get_first(splitRid, entryKey, entryRid)))
      return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, GET_FIRST_FAILED);

    *splitKey = entryKey;
    splitPage = splitPageNo;
    
    // Unpin LeafPage and SplitLeafPage 
    
    if (OK != (stat = MINIBASE_BM->unpinPage(pageNo, TRUE)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);

    if (OK != (stat = MINIBASE_BM->unpinPage(splitPageNo, TRUE)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
    
    return OK;
  }

}


Status BTreeFile::ifIndexPage(PageId pageNo, 
                              const void *key, 
                              const RID rid, 
                              void *splitKey, 
                              PageId& splitPage)

{
    
  
}
    
    
Status BTreeFile::insertElement(PageId pageNo, const void *key, const RID rid) {
  
    Status stat;
    PageId pageNum;
    RID curRid;

    // Pin the page with page number pageNo.

    if(OK != (stat = MINIBASE_BM->pinPage(pageNo, (Page *&)page)))
        return MINIBASE_CHAIN_ERROR(BUFMGR, stat);
    
    SortedPage *curPage = (SortedPage *)page;
    
    if (curPage->get_type() == INDEX) 
    {
      // Given current page is INDEX, so we need to find LEAF page number to insert record.
      BTIndexPage *pageIndex = (BTIndexPage *)curPage;
      PageId targetPage;

      if (OK != (stat = pageIndex->get_page_no(key, btpage->key_type, targetPage)))
          MINIBASE_CHAIN_ERROR(BTINDEXPAGE, GET_PAGE_NO_FAILED);
      
      // Call insertElement again to Insert this key, record in specified 
    }
}

Status BTreeFile::Delete(const void *key, const RID rid) {
  // put your code here
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  // put your code here
}
