/* -*- C++ -*- */
/*
 * btindex_page.h - definition of class BTIndexPage for Mini{base|rel} project.
 *
 */

#ifndef BTINDEX_PAGE_H
#define BTINDEX_PAGE_H

#include "minirel.h"
#include "page.h"
#include "sorted_page.h"
#include "bt.h"
#include <string.h>

// Define your error code for index page here
enum btIndexErrCodes  {INSERT_REC_ERROR, GET_RECORD_FAILED, GET_FIRST_FAILED, GET_NEXT_FAILED, GET_PAGE_NO_FAILED};

class BTIndexPage : public SortedPage {
 private:
   // No private variables should be declared.

 public:

// In addition to initializing the  slot directory and internal structure
// of the HFPage, this function sets up the type of the record page.

   void init(PageId pageNo)
   { HFPage::init(pageNo); set_type(INDEX); }

// ------------------- insertKey ------------------------
// Inserts a <key, page pointer> value into the index node.
// This is accomplished by a call to SortedPage::insertRecord()
// This function sets up the recPtr field for the call to
// SortedPage::insertRecord()
   
   Status insertKey(const void *key, AttrType key_type,PageId pageNo, RID& rid);


// ------------------- OPTIONAL: deletekey ------------------------
// this is optional, and is only needed if you want to do full deletion
   Status deleteKey(const void *key, AttrType key_type, RID& curRid);

// ------------------ get_page_no -----------------------
// This function encapsulates the search routine to search a
// BTIndexPage. It uses the standard search routine as
// described in the textbook, and returns the page_no of the
// child to be searched next.

   Status get_page_no(const void *key, AttrType key_type, PageId& pageNo);

    
// ------------------- Iterators ------------------------
// The two functions get_first and get_next provide an
// iterator interface to the records on a BTIndexPage.
// get_first returns the first <key, pageNo> pair from the page,
// while get_next returns the next pair on the page.
// These functions make calls to HFPage::firstRecord() and
// HFPage::nextRecord(), and then split the flat record into its
// two components: namely, the key and pageNo.
// Should return NOMORERECS when there are no more pairs.

   Status get_first(RID& rid, void *key, PageId& pageNo);
   Status get_next (RID& rid, void *key, PageId& pageNo);

// ------------------- Left Link ------------------------
// You will recall that index pages have a left-most
// pointer that is followed whenever the search key value
// is less than the least key value in the index node. The
// previous page pointer is used to implement the left link.

   PageId getLeftLink(void) { return getPrevPage(); }
   void   setLeftLink(PageId left) { setPrevPage(left); }

    
   // The remaining functions of SortedPage are still visible.
};

#endif
