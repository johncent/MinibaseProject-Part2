package heap;

import global.*;
import bufmgr.BufMgr;
import diskmgr.DiskMgr;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is an unordered set of records, stored on a set of pages. This
 * class provides basic support for inserting, selecting, updating, and deleting
 * records. Temporary heap files are used for external sorting and in other
 * relational operators. A sequential scan of a heap file (via the Scan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst
{
  protected static final short DATA_PAGE = 1; //arbitrary number to describe HFPage as a data page
  protected static final short DIR_PAGE = 2; //arbitrary number to describe HFPage as a directory page
  protected boolean heap_file_exists;
  protected boolean is_temp_file;
  protected PageId head_node;
  protected String name_of_file;
  
  /**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
   */
  public HeapFile( String name )
  {  
    // storing off the name of the incoming file for any later use
    name_of_file = name;
    
    if( name != null )
    {
      is_temp_file = false;
      head_node = Minibase.DiskManager.get_file_entry( name );
      
      if( head_node != null )
      {
        heap_file_exists = true;
      } // end if
      else
      {
        heap_file_exists = false;
      }
    }  // end if  
    else
    {
      is_temp_file = true;        
    } // end else

    // If the heap file does not already exists, we are ready
    // to make a directory page
      if( !heap_file_exists )
      {
      DirPage directory_page = new DirPage();
      head_node = Minibase.BufferManager.newPage( directory_page, 1 );
      directory_page.setCurPage( head_node );
      Minibase.BufferManager.unpinPage( head_node, true );
      
      // If this isn't a temporary file, add it to the database
      if( !is_temp_file )
      {
        Minibase.DiskManager.add_file_entry( name, head_node );
      } // end if   
      } // end if 
    
  } // end constructor

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable
  {
    if( is_temp_file )
    {
      deleteFile();
    } // end if
            
  } // end finalize

  /**
   * Deletes the heap file from the database, freeing all of its pages.
   */
  public void deleteFile()
  {
    PageId directory_id = new PageId( head_node.pid );
    DirPage directory_page = new DirPage();
    PageId next_page_id;
    
    for( ; directory_id.pid != -1; directory_id = next_page_id )
    {
      Minibase.BufferManager.pinPage( directory_id, directory_page, PIN_DISKIO );
      int count = directory_page.getEntryCnt();
      for( int i = 0; i < count; i++ )
      {
        PageId page_id = directory_page.getPageId( i );
        Minibase.BufferManager.freePage( page_id );
      } // end for loop
      
      next_page_id = directory_page.getNextPage();
      Minibase.BufferManager.unpinPage( directory_id, false );
      Minibase.BufferManager.freePage( directory_id );
    } // end for loop

    // TODO - missing if (!isTemp)
    
  } // end deleteFile

  /**
   * Inserts a new record into the file and returns its RID.
   * 
   * @throws IllegalArgumentException if the record is too large
   */
  public RID insertRecord(byte[] record) {

    if((record.length + 20 + 4) > PAGE_SIZE) //If record length + the header size + the slot size is greater than the size of a page
      throw new IllegalArgumentException("Record size is greater than space needed");

    PageId availablePageId = getAvailPage( record.length + 4 );

    DataPage availableDataPage = new DataPage();

    //Now that we know of an available page, buffer it to insert the record into it
    Minibase.BufferManager.pinPage(availablePageId, availableDataPage, PIN_DISKIO);

    RID rid = availableDataPage.insertRecord(record); //insert

    int freeSpace = availableDataPage.getFreeSpace();

    Minibase.BufferManager.unpinPage(availablePageId, true); //unpin as dirty

    //Update directory entry to account for added record and subtracted free space
    updateEntry(availablePageId, 1, freeSpace);

    return rid;

  }

  protected int findEntry(PageId pageno, PageId dirId, DirPage dirPage)
  {
    dirId.pid = head_node.pid;
    
    do
    {
       Minibase.BufferManager.pinPage( dirId, dirPage, PIN_DISKIO );

       int count = dirPage.getEntryCnt();

       for( int i = 0; i < count; i++ )
       {
         if( pageno.pid == dirPage.getPageId(i).pid )
          
           return i;
       }

       PageId nextId = dirPage.getNextPage();

       Minibase.BufferManager.unpinPage(dirId, false);

       dirId.pid = nextId.pid;

    } while(true);
  }
  /**
   * Updates the directory entry for the given page with the recently changed values 
   * (i.e. delta is the difference between the old and the new).
   * @param pageno Page id of data page that was updated
   * @param deltaRec Difference in records
   * @param freecnt The page's updated free count 
   */
  private void updateEntry(PageId pageno, int deltaRec, int freecnt)
  {
    PageId directory_id = new PageId();

    DirPage directory_page = new DirPage();

    int index = findEntry( pageno, directory_id, directory_page );

    int record_count = directory_page.getRecCnt( index ) + deltaRec;

    if ( record_count < 1 )
    {
      deletePage( pageno, directory_id, directory_page, index );
    }
    else
    {
      directory_page.setRecCnt( index, (short) record_count );

      directory_page.setFreeCnt( index, (short) freecnt );

      Minibase.BufferManager.unpinPage( directory_id, true );
    }
  }

  /**
   * Searches through the directory for a page with enough available space to store a record of the given
   * size. Creates a new data page if no pages with enough available space are found
   *
   * @param recLen Length of a record we are trying to add
   * @return PageId of the available page 
   */
  private PageId getAvailPage(int recLen)
  {
    PageId availPageId = null;
    PageId directory_id = new PageId( head_node.pid ); //Start at the head of the file
    DirPage directory_page = new DirPage();
    PageId next_page_id;

    while( directory_id.pid != -1 )
    {
      Minibase.BufferManager.pinPage( directory_id, directory_page, PIN_DISKIO );
       
      for(int slot = 0; slot < directory_page.getEntryCnt(); slot++)
      {
        int freeSpace = directory_page.getFreeCnt(slot);
        if(recLen + 4 <= freeSpace) //Add 4 to account for the slot size needed for the record
        {
          availPageId = directory_page.getPageId(slot);
          break; //Found available page
        }
      }

      next_page_id = directory_page.getNextPage();
      Minibase.BufferManager.unpinPage(directory_id, false); //Unpin clean, nothing was changed in page
      if(null != availPageId)
      {
        break; //Leave while loop, you have your page
      }
      else
      {
        directory_id = next_page_id;
      }
    }

    if(null == availPageId) //If we don't have an available page, insert a new one
    {
      availPageId = insertPage();
    }
    return availPageId;

  }

  /**
   * Inserts a new data page its directory entry into the heap file. If necessary, this also inserts a new directory page.
   * @return Id of the new data page
   */
  private PageId insertPage()
  {
    boolean found_dir_page = false; //flag that says whether we found a dir page with space for a new entry or not
    PageId directory_id = new PageId( head_node.pid ); //Start at the head of the file
    DirPage directory_page = new DirPage();
    PageId next_page_id;
    short entry_count = 0; //Number of entries on directory page

    while (true)
    {
      Minibase.BufferManager.pinPage( directory_id, directory_page, PIN_DISKIO );
       
       entry_count = directory_page.getEntryCnt();
       if(entry_count < directory_page.MAX_ENTRIES)
       {
          found_dir_page = true;
          break;
       }

      next_page_id = directory_page.getNextPage();
      if(-1 == next_page_id.pid)
      { //No available directory pages with empty entry slots, we need to create one
        DirPage new_directory_page = new DirPage();
        PageId new_directory_id = Minibase.BufferManager.newPage(new_directory_page, 1);
        new_directory_page.setCurPage(new_directory_id);
        directory_page.setNextPage(new_directory_id);
        new_directory_page.setPrevPage(directory_id);
        Minibase.BufferManager.unpinPage(directory_id, true); //Unpin dirty
        directory_id = new_directory_id;
        directory_page = new_directory_page;
        break;
      }
      else
      {
        Minibase.BufferManager.unpinPage(directory_id, false); //Unpin clean, nothing was changed in page
        directory_id = next_page_id;
      }
    }

    if( !found_dir_page )
    { //We created a new directory page with no entries
      entry_count = 0;
    }

    DataPage new_data_page = new DataPage();
    PageId new_data_id = Minibase.BufferManager.newPage(new_data_page, 1);
    new_data_page.setCurPage(new_data_id);
    directory_page.setPageId(entry_count, new_data_id); //set the available entry to our new data page
    directory_page.setRecCnt(entry_count, (short)0); //record count initialized to 0 for new data page
    short free_space = new_data_page.getFreeSpace();
    directory_page.setFreeCnt(entry_count, free_space); //initialize free space
    directory_page.setEntryCnt(++entry_count); //increment number of entries
    Minibase.BufferManager.unpinPage(new_data_id, true);
    Minibase.BufferManager.unpinPage(directory_id, true);

    return new_data_id;
  }

  protected void deletePage( PageId pageno, PageId dirId, DirPage dirPage, int index )
  {
    Minibase.BufferManager.freePage( pageno );

    dirPage.compact( index );

    short count = dirPage.getEntryCnt();

    if( count == 1 && dirId.pid != head_node.pid )
    {
      DirPage directory_page = new DirPage();

      PageId prevId = dirPage.getPrevPage();

      PageId nextId = dirPage.getNextPage();

      if( prevId.pid != -1 )
      {
        Minibase.BufferManager.pinPage( prevId, directory_page, PIN_DISKIO );

        directory_page.setNextPage( nextId );

        Minibase.BufferManager.unpinPage( prevId, true );
      }
      if( nextId.pid != -1 )
      {
        Minibase.BufferManager.pinPage( nextId, directory_page, PIN_DISKIO );
     
        directory_page.setPrevPage( prevId );
     
        Minibase.BufferManager.unpinPage( nextId, true );
      }

      Minibase.BufferManager.unpinPage( dirId, false );

      Minibase.BufferManager.freePage( dirId );
    }
    else
    {
      --count;

      dirPage.setEntryCnt( count );

      Minibase.BufferManager.unpinPage( dirId, true );
    }
  }

  /**
   * Reads a record from the file, given its id.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid)
  {

    DataPage data_page;
  
    data_page = new DataPage();
  
    Minibase.BufferManager.pinPage( rid.pageno, data_page, PIN_DISKIO );
  
    byte record_data[];
  
    try
    {
      record_data = data_page.selectRecord( rid );
    }
    catch(IllegalArgumentException exc)
    {
      throw exc;
    }
    
    Minibase.BufferManager.unpinPage( rid.pageno, false );
  
    return record_data;
  }

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public void updateRecord(RID rid, byte[] newRecord)
  {
    DataPage data_page = new DataPage();
    
    Minibase.BufferManager.pinPage( rid.pageno, data_page, PIN_DISKIO );
    
    try
    {
      data_page.updateRecord( rid, newRecord );
    
      Minibase.BufferManager.unpinPage( rid.pageno, true );
    }
    catch(IllegalArgumentException exc)
    {
      Minibase.BufferManager.unpinPage( rid.pageno, false );

      throw exc;
    }
  }

  /**
   * Deletes the specified record from the heap file.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public void deleteRecord(RID rid)
  {
    DataPage data_page = new DataPage();
  
    Minibase.BufferManager.pinPage( rid.pageno, data_page, PIN_DISKIO );
  
    try
    {
      data_page.deleteRecord( rid );
  
      short freecnt = data_page.getFreeSpace();
  
      Minibase.BufferManager.unpinPage( rid.pageno, true );
  
      updateEntry( rid.pageno, -1, freecnt );
    }
    catch(IllegalArgumentException exc)
    {
      Minibase.BufferManager.unpinPage( rid.pageno, false );
 
      throw exc;
    }
  }

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {
    int rec_count = 0; //Initialize number of records in file

    PageId directory_id = new PageId( head_node.pid ); //Start at the head of the file
    DirPage directory_page = new DirPage();
    PageId next_page_id;

    //Iterate through the directory pages
    while( directory_id.pid != -1 )
    {
      Minibase.BufferManager.pinPage( directory_id, directory_page, PIN_DISKIO );
       
      //Iterate through the directory entries
      for(int slot = 0; slot < directory_page.getEntryCnt(); slot++)
      {
        rec_count = rec_count + directory_page.getRecCnt(slot);
      }

      next_page_id = directory_page.getNextPage();
      Minibase.BufferManager.unpinPage(directory_id, false); //Unpin clean, nothing was changed in page
      directory_id = next_page_id;
    }

    return rec_count;

  }

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {    
    return name_of_file;
  }

} // public class HeapFile implements GlobalConst
