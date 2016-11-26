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
    
    //throw new UnsupportedOperationException("Not implemented");
  } // end deleteFile

  /**
   * Inserts a new record into the file and returns its RID.
   * 
   * @throws IllegalArgumentException if the record is too large
   */
  public RID insertRecord(byte[] record) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Reads a record from the file, given its id.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public void updateRecord(RID rid, byte[] newRecord) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Deletes the specified record from the heap file.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public void deleteRecord(RID rid) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {
    throw new UnsupportedOperationException("Not implemented");
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
