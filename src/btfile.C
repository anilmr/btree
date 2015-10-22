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
  // put your code here
  return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {
  // put your code here
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  // put your code here
}
