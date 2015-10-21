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
  Datatype *dataType = new Datatype;
  int recLen;

  // As pageNo is the data payload in Index pages, add this to dataType union.
  dataType->pageNo = pageNo;

  //Create the <key, pageNo> pair using make_entry function.
  make_entry(target, key_type, key, INDEX, *dataType, &recLen);

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

  Status stat;
  RID rid;
  PageId firstPage;
  Keytype firstKey;
  Keytype curKey;
  Keytype targetKey;
  PageId targetPage;
  PageId curPage;
  int numRec;
  int i=0;

  // Check if the key is NULL, if key is NULL, then provide current index page number.
  if(key == NULL)
  {
    pageNo = page_no();
    return OK;
  }

  // Get the first record in this index page.

  if (OK != (stat = get_first(rid, &firstKey, firstPage)))
    return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_FIRST_FAILED);

  if (keyCompare(key, &firstKey, key_type) < 0)
  {
    // Given key is less than the first key stored in this page. Hence pageNo is nothing but the left pointer. 
    pageNo = getLeftLink();
    return OK;
  }
  else 
  {
    curKey = firstKey;
    curPage = firstPage;
    // Well given key is greater than or equal to firstKey. So time to check where given key fits the bill. 
    numRec = numberOfRecords();

    while(i < (numRec - 1))
    {
      if (OK != (stat = get_next(rid, &targetKey, targetPage)))
        break;

      if ((keyCompare(key, &curKey, key_type) >= 0) && (keyCompare(key, &targetKey, key_type) < 0)) 
      {
        pageNo = curPage;
        return OK;
      }
    
      curKey = targetKey;
      curPage = targetPage;
      ++i; 
    }
  
    pageNo = curPage;
    return OK;
  }

  return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_PAGE_NO_FAILED);
}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
  char *target;
  int recLen;
  Status stat;
  KeyDataEntry *keyData = new KeyDataEntry;
  Datatype *dataType = new Datatype;
  
  // Get the RID of the first record in the current page. 
  if (OK != (stat = firstRecord(rid)))
    return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_FIRST_FAILED);

  // Collect the <key, pageNo> data stored in that RID to temporary data holder.

  if(OK != (stat = returnRecord(rid, target, recLen)))
    return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_FIRST_FAILED);

  // Once the data is available, use get_key_data function defined in key.C to get 
  // the pageNo attached to the key.

  keyData = (KeyDataEntry *)target;
  get_key_data(key, dataType, keyData, recLen, INDEX);
  pageNo = dataType->pageNo;
  return OK;
}


Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{

  // Use nextRecord function to find the next page.

  char *target;
  RID curRid;
  int recLen;
  Status stat;
  KeyDataEntry *keyData = new KeyDataEntry;
  Datatype *dataType = new Datatype;

  if (OK != (stat = nextRecord(rid, curRid)))
  { 
    pageNo = INVALID_PAGE;
    return DONE;
  }

  if (OK != (stat = returnRecord(curRid, target, recLen)))
    return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GET_NEXT_FAILED);

  keyData = (KeyDataEntry *)target;
  get_key_data(key, dataType, keyData, recLen, INDEX);
  rid = curRid;
  pageNo = dataType->pageNo;

  return OK;
}
