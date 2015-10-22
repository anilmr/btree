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



Status BTreeFile::ifLeafPage(PageId pageNo, 
                              const void *key, 
                              const RID rid, 
                              void *splitKey, 
                              PageId& splitPage)

{
  Status 

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
