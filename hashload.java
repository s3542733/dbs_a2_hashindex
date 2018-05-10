import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class hashload implements dbimpl {

	public static void main(String[] args) {
		hashload load = new hashload();
		long startTime = System.currentTimeMillis();
		load.readArguments(args);
		long endTime = System.currentTimeMillis();
		System.out.println("Load time: " + (endTime - startTime) + "ms.");
	}
	
	// checks to see if pagesize argument from commandline
	// is an integer
	public void readArguments(String args[]) {
		if (args.length == 1) {
			if (isInteger(args[0])) {
				loadHash(Integer.parseInt(args[0]));
			} else {
				System.out.println("Usage: java hashload [page_size]");
			}
		} else {
			System.out.println("Usage: java hashload [page_size]");
		}
	}
	
	// checks to see if s can be converted to
	// an integer
	public boolean isInteger(String s) {

		boolean isValidInt = false;
		try {
			Integer.parseInt(s);
			isValidInt = true;
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}

		return isValidInt;
	}

	// calculates the number of records in heapfile based on the last
	// pagenum and the pagesize
	public int countRecords(int pageSize, File heapFile) {

		FileInputStream fis = null;
		int numOfRecords, recCount, recOffset, pageNum;
		numOfRecords = recCount = recOffset = pageNum = 0;
		int recordsPerPage = pageSize / RECORD_SIZE;
		long heapFileSize = heapFile.length();
		long offset = heapFileSize - pageSize;
		boolean hasNextRecord = true;
		try {
			byte[] bLastPage = new byte[pageSize];
			byte[] bPageNum = new byte[EOF_PAGENUM_SIZE];
			fis = new FileInputStream(heapFile);
			fis.skip(offset);
			fis.read(bLastPage, 0, pageSize);
			System.arraycopy(bLastPage, pageSize - EOF_PAGENUM_SIZE, bPageNum, 0, EOF_PAGENUM_SIZE);
			pageNum = ByteBuffer.wrap(bPageNum).getInt();
			while (hasNextRecord) {
				try {
					byte[] bRecord = new byte[RECORD_SIZE];
					byte[] bRid = new byte[RID_SIZE];
					int rid = 0;
					System.arraycopy(bLastPage, recOffset, bRecord, 0, RECORD_SIZE);
					System.arraycopy(bRecord, 0, bRid, 0, RID_SIZE);
					rid = ByteBuffer.wrap(bRid).getInt();
					if (rid < recCount) {
						hasNextRecord = false;
					} else {
						recOffset += RECORD_SIZE;
						recCount++;
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					hasNextRecord = false;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		numOfRecords = (pageNum * recordsPerPage) + recCount;
		return numOfRecords;
	}

	// creates an empty byte array equal to all buckets multiplied
	// by the size of a single bucket
	public byte[] initializeIndex(int numOfBuckets) {

		byte[] hashIndex = new byte[numOfBuckets * BUCKET_VAL_SIZE];
		return hashIndex;
	}
	
	// calculates the number of buckets required to achieve
	// desired occupancy. occupancy is an integer and is defined
	// in the dbimpl interface
	public int calcNumOfBuckets(int numOfRecords) {

		float numOfBuckets = 0;
		numOfBuckets = (float) numOfRecords * ((float) 100 / (float) IDEAL_OCCUPANCY);
		return (int) numOfBuckets;
	}

	// simple hash function using hashcode. returns the bucket number
	// that the record is to be stored in by calculating mod numOfBuckets
	public int hashKey(String searchKey, int numOfBuckets) {

		float index = 0;
		int hash = 0;

		hash = searchKey.hashCode();
		index = Math.abs(hash % numOfBuckets);
		return (int)index;
	}
	
	
	// returns the offset of a bucket in the hashindex by
	// multiplying the bucket number with bucket size
	public int getBucketOffset(int bucketNum) {

		int bucketOffset = 0;

		bucketOffset = bucketNum * BUCKET_VAL_SIZE;
		return bucketOffset;
	}
	
	// checks to see if a bucket is full by comparing it with
	// an empty bucket
	public boolean isBucketFull(byte[] hashIndex, int bucketOffset) {

		boolean isFull = true;
		byte[] bucketVal = new byte[BUCKET_VAL_SIZE];
		byte[] emptyBucket = new byte[BUCKET_VAL_SIZE];
		System.arraycopy(hashIndex, bucketOffset, bucketVal, 0, BUCKET_VAL_SIZE);
		if (Arrays.equals(bucketVal, emptyBucket)) {
			isFull = false;
		}
		return isFull;
	}
	
	// attempts to store a record in a bucket
	public byte[] storeInBucket(byte[] hashIndex, String recName, int pageNum, int rid, int pageSize,
			int numOfBuckets) {

		int bucketNum = hashKey(recName, numOfBuckets);
		byte[] bucketVal = tobucketVal( pageNum, rid, pageSize);
		boolean stored = false;
		while (!stored) {
			int bucketOffset = getBucketOffset(bucketNum);
			if (isBucketFull(hashIndex, bucketOffset)) {
				if (bucketNum == numOfBuckets - 1) {
					bucketNum = 0;
				} else {
					bucketNum++;
				}
			} else {
				System.arraycopy(bucketVal, 0, hashIndex, bucketOffset, BUCKET_VAL_SIZE);
				stored = true;
			}
		}
		return hashIndex;
	}
	
	// sanity check method to make sure that all the records are stored in the hashindex
	// prints out how many records are found against the numOfRecords. This method is
	// called after the hashindex is written to file.
	public void checkHashRecords(int pageSize, int numOfRecords, int numOfBuckets) {

		FileInputStream fis = null;
		int recordCount = 0;
		boolean eof = false;
		byte[] emptybucket = new byte[BUCKET_VAL_SIZE];
		byte[] hashIndex = new byte[numOfBuckets * BUCKET_VAL_SIZE];
		try {
			fis = new FileInputStream(new File(HASH_FNAME + pageSize));
			byte[] keyval = new byte[BUCKET_VAL_SIZE];
			fis.read(hashIndex, 0, numOfBuckets * BUCKET_VAL_SIZE);
			int offset = 0;
			while (!eof) {
				System.arraycopy(hashIndex, offset, keyval, 0, BUCKET_VAL_SIZE);
				if (!Arrays.equals(keyval, emptybucket)) {
					recordCount++;
				}
				offset += BUCKET_VAL_SIZE;
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			eof = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(recordCount + "/" + numOfRecords + " in the hashIndex.");
	}
	
	
	// transforms data from record into data for the bucket by calculating the
	// record offset from rid, pageNum and pageSize
	public byte[] tobucketVal(int pageNum, int rid, int pageSize) {

		byte[] bucketVal = new byte[BUCKET_VAL_SIZE];
		byte[] bBufferChar = new byte[BUFFER_CHARACTER_SIZE];
		byte[] bRecOffset = new byte[HEAP_FOFFSET_SIZE];
		int recordOffset = (pageNum * pageSize) + (rid * RECORD_SIZE);
		
		bBufferChar = BUFFER_CHARACTER.getBytes();
		bRecOffset = ByteBuffer.allocate(HEAP_FOFFSET_SIZE).putInt(recordOffset).array();
		
		System.arraycopy(bBufferChar, 0, bucketVal, 0, BUFFER_CHARACTER_SIZE);
		System.arraycopy(bRecOffset, 0, bucketVal, BUFFER_CHARACTER_SIZE, HEAP_FOFFSET_SIZE);

		return bucketVal;
	}
	
	// reads every record from the heapfile and stores it into the hashindex
	// to be written to file. this function uses 
	public void loadHash(int pageSize) {
		File heapFile = new File(HEAP_FNAME + pageSize);
		int pageCount, recCount, recordLen, rid, pageNum;
		pageCount = recCount = recordLen = rid = pageNum = 0;
		boolean isNextPage = true;
		boolean isNextRecord = true;
		
		// key variables for calculating bucket offsets
		int numOfRecords = countRecords(pageSize, heapFile);
		int numOfBuckets = calcNumOfBuckets(numOfRecords);
		// creates an empty byte array
		byte[] hashIndex = initializeIndex(numOfBuckets);
		
		try {
			FileInputStream fis = new FileInputStream(heapFile);
			// reading page by page from heapfile
			while (isNextPage) {
				byte[] bPage = new byte[pageSize];
				byte[] bPageNum = new byte[EOF_PAGENUM_SIZE];
				fis.read(bPage, 0, pageSize);
				// store pagenum to pass to storeInBucket()
				System.arraycopy(bPage, pageSize - EOF_PAGENUM_SIZE, bPageNum, 0, EOF_PAGENUM_SIZE);
				pageNum = ByteBuffer.wrap(bPageNum).getInt();

				// reading by record, return true to read the next record
				isNextRecord = true;
				while (isNextRecord) {
					byte[] bRecord = new byte[RECORD_SIZE];
					byte[] bRid = new byte[RID_SIZE];
					byte[] bRecName = new byte[BN_NAME_SIZE];
					String recName = "";
					try {
						System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
						// store rid to pass to storeInBucket()
						System.arraycopy(bRecord, 0, bRid, 0, RID_SIZE);
						// store recName to pass to storeInBucket()
						System.arraycopy(bRecord, BN_NAME_OFFSET, bRecName, 0, BN_NAME_SIZE);
						rid = ByteBuffer.wrap(bRid).getInt();
						recName = new String(bRecName).trim();
						if (rid != recCount) {
							isNextRecord = false;
						} else {
							try {
								// attempts to store record in a bucket in hashIndex
								hashIndex = storeInBucket(hashIndex, recName, pageNum, rid, pageSize, numOfBuckets);
							} catch (Exception e) {
								e.printStackTrace();
							}
							recordLen += RECORD_SIZE;
							recCount++;
						}
						// if recordLen exceeds pagesize, catch this to reset to next page
					} catch (ArrayIndexOutOfBoundsException e) {
						isNextRecord = false;
						recordLen = recCount = rid = 0;
					}
				}
				// check to complete all pages
				if (pageNum != pageCount) {
					isNextPage = false;
				}
				pageCount++;
			}
			fis.close();
			
			// writes hashindex to a file
			hashIndexToFile(hashIndex, pageSize);
			
			// Uncomment to check if all records have been loaded into hashFile
			// properly.
			checkHashRecords(pageSize, numOfRecords, numOfBuckets);
		
		} catch (FileNotFoundException e) {
			System.out.println("File: " + heapFile.getName() + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// writes hashindex to a file
	public void hashIndexToFile(byte[] hashIndex, int pageSize) {

		FileOutputStream fos;
		File hashFile;
		try {
			hashFile = new File(HASH_FNAME + pageSize);
			fos = new FileOutputStream(hashFile);
			fos.write(hashIndex);
			fos.flush();
			fos.close();
			System.out.println("HashFile written successfully!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}