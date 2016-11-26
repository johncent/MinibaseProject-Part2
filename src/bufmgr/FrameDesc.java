package bufmgr;

import global.GlobalConst;

/**
 * <h3>Frame Descriptor</h3>
 * The frame descriptor is a structure to describe the various states and other
 * information associated with a frame of a buffer pool.
 * <ol>
 * <li>Contains accessor and mutator methods for various fields
 * </ol>
 * Frame descriptors will be used by the Buffer Manager
 */
public class FrameDesc implements GlobalConst {

  private boolean dirtyBit; //true = frame is dirty
  private boolean validData; //true = frame contains valid data
  private int diskPageNum;
  private int pinCount;
  private boolean refBit; //true = page recently referenced


  /**
  * Constructor for FrameDesc class
  */
  public FrameDesc(){

    dirtyBit = false;
    validData = false;
    diskPageNum = INVALID_PAGEID;
    pinCount = 0;
    refBit = false;
    }

  /**
  * Sets dirtyBit field 
  * @param dirty True/False value to set dirtyBit to
  */
    public void setDirtyBit(boolean dirty)
    {
      dirtyBit = dirty;
    }

  /**
  * Sets dirtyBit field to false
  */
    public void clearDirtyBit()
    {
      dirtyBit = false;
    }

  /**
  * Gets dirtyBit field.
  * @return True if dirty, false if not
  */
    public boolean isDirty()
    {
      return dirtyBit;
    }

  /**
  * Sets validData field 
  * @param bool True/False value to set validData to
  */
    public void setValidDataIndicator(boolean bool)
    {
      validData = bool;
    }

  /**
  * Sets validData field to false. This frame does not have
  * data which reflects data in a disk page.
  */
    public void clearValidDataIndicator()
    {
      validData = false;
    }

  /**
  * Gets validData field
  */
    public boolean getValidDataIndicator()
    {
      return validData;
    }

  /**
  * Sets diskPageNum to supplied value
  * @param num Page number of the page occupying this frame
  */
    public void setDiskPageNum(int num)
    {
      diskPageNum = num;
    }

  /**
  * Gets disk page number of page occupying this frame
  */
    public int getDiskPageNum()
    {
      return diskPageNum;
    }

    /**
  * Increment the pin count on this frame's page
  */
    public void incrementPinCount()
    {
      pinCount++;
    }

  /**
  * Decrement the pin count on this frame's page
  * If it is already 0, do nothing
  */
    public void decrementPinCount()
    {
      if(pinCount > 0)
      {
        pinCount--;
      }
    }

  /**
  * Get the pin count on this frame's page
  */
    public int getPinCount()
    {
      return pinCount;
    }

  /**
  * Set refBit value
  * @param ref True/False value to set ref bit to
  */
    public void setRefBit(boolean ref)
    {
      refBit = ref;
    }

  /**
  * Get the refBit
  */
    public boolean getRefBit()
    {
      return refBit;
    }

}