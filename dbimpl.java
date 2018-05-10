/**
 * Database Systems - HEAP IMPLEMENTATION
 */

public interface dbimpl {

	public static final String HEAP_FNAME = "heap.";
	public static final String HASH_FNAME = "hash.";
	public static final String ENCODING = "utf-8";

	// fixed/variable lengths
	public static final int RECORD_SIZE = 297;
	public static final int RID_SIZE = 4;
	public static final int REGISTER_NAME_SIZE = 14;
	public static final int BN_NAME_SIZE = 200;
	public static final int BN_STATUS_SIZE = 12;
	public static final int BN_REG_DT_SIZE = 10;
	public static final int BN_CANCEL_DT_SIZE = 10;
	public static final int BN_RENEW_DT_SIZE = 10;
	public static final int BN_STATE_NUM_SIZE = 10;
	public static final int BN_STATE_OF_REG_SIZE = 3;
	public static final int BN_ABN_SIZE = 20;
	public static final int EOF_PAGENUM_SIZE = 4;

	// to prevent OutOfMemoryErrors keep
	// IDEAL_OCCUPANCY >= 40
	public static final int IDEAL_OCCUPANCY = 70;

	// public static final int HEAP_FOFFSET_SIZE = 8;
	// public static final int BUCKET_KEYVAL_SIZE = BN_NAME_SIZE + HEAP_FOFFSET_SIZE;

	public static final int HEAP_FOFFSET_SIZE = 4;
	public static final String BUFFER_CHARACTER = "B";
	public static final int BUFFER_CHARACTER_SIZE = 1;
	public static final int BUCKET_VAL_SIZE =  BUFFER_CHARACTER_SIZE + HEAP_FOFFSET_SIZE;
	
	public static final int BN_NAME_OFFSET = RID_SIZE + REGISTER_NAME_SIZE;

	public static final int BN_STATUS_OFFSET = RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE;

	public static final int BN_REG_DT_OFFSET = RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE;

	public static final int BN_CANCEL_DT_OFFSET = RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE
			+ BN_REG_DT_SIZE;

	public static final int BN_RENEW_DT_OFFSET = RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE
			+ BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE;

	public static final int BN_STATE_NUM_OFFSET = RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE
			+ BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE + BN_RENEW_DT_SIZE;

	public static final int BN_STATE_OF_REG_OFFSET = RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE
			+ BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE + BN_RENEW_DT_SIZE + BN_STATE_NUM_SIZE;

	public static final int BN_ABN_OFFSET = RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE
			+ BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE + BN_RENEW_DT_SIZE + BN_STATE_NUM_SIZE + BN_STATE_OF_REG_SIZE;

	public void readArguments(String args[]);

	public boolean isInteger(String s);

}
