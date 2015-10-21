/*
 * btindex_page.cc - implementation of class BTIndexPage
 *
 */

#include "btindex_page.h"

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{
  // Create a new KeyDataEntry object to hold the key + pageNo 
  KeyDataEntry *target = new KeyDataEntry;
  DataType *dataType = new DataType;
  int recLen;

  // As pageNo is the data payload in Index pages, add this to dataType union.
  dataType->pageNo = pageNo;

  //Create the <key, pageNo> pair using make_entry function.
  make_entry(target, key_type, key, INDEX, *dataType, &recLen)

  //Use insertRecord of sorted_page class to insert the data stored in target region.

  if ( OK != insertRecord(key_type, (char *)target, recLen, rid))
        return MINIBASE_FIRST_ERROR(BTINDEXPAGE,INSERT_REC_FAILED);

  return OK;
}


Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
  
  if (OK != deleteRecord(curRid))
        return MINIBASE_FIRST_ERROR(BTINDEXPAGE,DELETE_REC_FAILED);

  return OK;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
  
  return OK;
}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
  // put your code here
  return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
  // put your code here
  return OK;
}
