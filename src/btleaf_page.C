/*
 * btleaf_page.cc - implementation of class BTLeafPage
 *
 */

#include "btleaf_page.h"

const char* BTLeafErrorMsgs[] = {
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
Status BTLeafPage::insertRec(const void *key,
                              AttrType key_type,
                              RID dataRid,
                              RID& rid)
{ 
  
  if (key == NULL)
    return MINIBASE_FIRST_ERROR(BTLEAFPAGE, INSERT_REC_ERROR);
  
  KeyDataEntry *target = new KeyDataEntry;
  Datatype *dataType = new Datatype;
  int recLen;
  Status stat;
  
  dataType->rid = dataRid;
  
  make_entry(target, key_type, key, LEAF, *dataType, &recLen);

  if (OK != (stat = insertRecord(key_type, (char *)target, recLen, rid)))
    return MINIBASE_FIRST_ERROR(BTLEAFPAGE, INSERT_REC_FAILED);

  return OK;
}


Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID & dataRid)
{
  // Scan through all records in current page and decode the dataRid from matching key record. 
  // Use get_first and get_next to sequentially scan through all records.
  
  RID rid;
  Keytype curKey;
  RID dataRid;
  Status stat;

  if (OK != (stat = get_first(rid, &curKey, dataRid)))
      return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_FIRST_FAILED);

  if (keyCompare(curKey, key, key_type) == 0)
      return OK;

  else
  {
    while(OK == (stat = get_next(rid, &curKey, dataRid)))
    {
      if (keyCompare(curKey, key, key_type) == 0)
          return OK;
    }
  }
  // We scanned thorugh all records in this page, and key is not present.
  return DONE;
}


Status BTLeafPage::get_first (RID& rid,
                              void *key,
                              RID & dataRid)
{ 
  char *target;
  int recLen;
  Status stat;
  KeyDataEntry *keyData = new KeyDataEntry;
  Datatype *dataType = new Datatype;

  if (OK != (stat = firstRecord(rid)))
    return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_FIRST_FAILED);

  if (OK != (stat = returnRecord(rid, target, recLen)))
    return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_RECORD_FAILED);

  keyData = (KeyDataEntry *)target;
  get_key_data(key, dataType, keyData, recLen, LEAF);
  
  dataRid = dataType->rid;
  return OK;
}


Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{

  char *target;
  int recLen;
  RID curRid;
  Status stat;
  KeyDataEntry *keyData = new KeyDataEntry;
  Datatype *dataType = new Datatype;

  if (OK != (stat = nextRecord(rid, curRid)))
    return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_NEXT_FAILED);

  if (OK != (stat = returnRecord(rid, target, recLen)))
      return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GET_RECORD_FAILED);

  keyData = (KeyDataEntry *)target;
  get_key_data(key, dataType, keyData, recLen, LEAF);
  rid = curRid;
  dataRid = dataType->rid;
  return OK;
}
