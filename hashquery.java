import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class hashquery implements dbimpl {

	public static void main(String[] args) {
		hashquery query = new hashquery();
		long startTime = System.currentTimeMillis();
		query.readArguments(args);
		long endTime = System.currentTimeMillis();
		System.out.println("Query time: " + (endTime - startTime) + "ms.");
	}
	
	// reads in arguments to check that there is a search term
	// and valid pagesize
	public void readArguments(String args[]) {
		if (args.length == 2) {
			if (isInteger(args[1])) {
				queryHash(args[0].trim(), Integer.parseInt(args[1]));
			} else {
				System.out.println("Usage: java hashquery [search_term] [page_size]");
			}
		} else {
			System.out.println("Usage: java hashquery [search_term] [page_size]");
		}
	}

	// checks to see if s can be converted
	// to an integer
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
		} catch (FileNotFoundException e) {
			System.out.println("File: " + heapFile.getName() + " not found.");
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		numOfRecords = (pageNum * recordsPerPage) + recCount;
		return numOfRecords;
	}
	
	// calculates the number of buckets required to achieve
	// desired occupancy. occupancy is an integer and is defined
	// in the dbimpl interface
	public int calcNumOfBuckets(int numOfRecords, int idealOccupancy) {

		float numOfBuckets = 0;
		numOfBuckets = (float) numOfRecords * ((float) 100 / (float) idealOccupancy);
		return (int) numOfBuckets;
	}
	
	// simple hash function using hashcode. returns the bucket number
	// that the record is to be stored in by calculating mod numOfBuckets
	public int hashKey(String searchKey, int numOfBuckets) {

		int index = 0;
		int hash = 0;

		hash = searchKey.hashCode();
		index = Math.abs(hash % numOfBuckets);
		return index;
	}
	
	// returns the offset of a bucket in the hashindex by
	// multiplying the bucket number with bucket size
	public int getBucketOffset(int bucketNum) {

		int bucketOffset = 0;

		bucketOffset = bucketNum * BUCKET_VAL_SIZE;
		return bucketOffset;
	}
		
	// converts the byte[] containing the bucket val
	// which is a long and returns it
	public int getBucketVal(byte[] bucketVal) {

		int heapOffset = 0;
		byte[] bHeapOffset = new byte[HEAP_FOFFSET_SIZE];

		System.arraycopy(bucketVal, BUFFER_CHARACTER_SIZE, bHeapOffset, 0, HEAP_FOFFSET_SIZE);
		heapOffset = ByteBuffer.wrap(bHeapOffset).getInt();

		return heapOffset;
	}
	
	// intializes the hashFile, heapFile and important variables
	// and passes them to the linear probing function for querying
	public void queryHash(String searchKey, int pageSize) {
		
		File hashFile = new File(HASH_FNAME + pageSize);
		File heapFile = new File(HEAP_FNAME + pageSize);
		int numOfRecords = countRecords(pageSize, heapFile);
		int numOfBuckets = calcNumOfBuckets(numOfRecords, IDEAL_OCCUPANCY);
		int bucketNum = hashKey(searchKey, numOfBuckets);
		
		linearProbe(searchKey, bucketNum, numOfBuckets, heapFile, hashFile);
	}
	
	// checks to see if a bucket is full or not
	public boolean bucketIsFull(int bucketNum, File hashFile) {

		FileInputStream hashFis = null;
		int bucketOffset = 0;
		boolean isFull = true;
		byte[] emptyBucket = new byte[BUCKET_VAL_SIZE];
		try {
			bucketOffset = getBucketOffset(bucketNum);
			hashFis = new FileInputStream(hashFile);
			hashFis.skip((long) bucketOffset);
			byte[] bucketVal = new byte[BUCKET_VAL_SIZE];
			hashFis.read(bucketVal, 0, BUCKET_VAL_SIZE);
			if (Arrays.equals(emptyBucket, bucketVal)) {
				isFull = false;
			}
		} catch (FileNotFoundException e) {
			System.out.println("File: " + hashFile.getName() + " not found.");
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return isFull;
	}
	
	// linear probing implementation that steps through buckets to check if
	// the key matches the searchkey and retrieves data if a match is found or
	// terminates if the bucket found is empty
	public void linearProbe(String searchKey, int bucketNum, int numOfBuckets, 
			File heapFile, File hashFile) {

		int originalBucketNum = bucketNum;
		int bucketOffset = 0;
		FileInputStream hashFis = null;
		try {
			while (bucketIsFull(bucketNum, hashFile)) {
				byte[] bucketVal = new byte[BUCKET_VAL_SIZE];
				bucketOffset = getBucketOffset(bucketNum);
				hashFis = new FileInputStream(hashFile);
				hashFis.skip((long) bucketOffset);
				hashFis.read(bucketVal, 0, BUCKET_VAL_SIZE);
				retrieveRecord(searchKey, getBucketVal(bucketVal), heapFile);
				// loops around if bucket number exceeds
				// the total number of buckets - 1
				if (bucketNum != numOfBuckets - 1) {
					bucketNum++;
				} else {
					bucketNum = 0;
				}
				// if a full loop is made then terminate,
				// there is no match/remaining matches
				if (bucketNum == originalBucketNum) {
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// uses the recordoffset and the heapfile to get the
	// record from the heapfile and print it out
	public void retrieveRecord(String searchKey, long recordOffset, File heapFile) {

		FileInputStream heapFis;
		byte[] bRecord = new byte[RECORD_SIZE];
		byte[] bRecordContents = new byte[RECORD_SIZE - RID_SIZE];
		byte[] bRecName = new byte[BN_NAME_SIZE];
		try {
			heapFis = new FileInputStream(heapFile);
			heapFis.skip(recordOffset);
			heapFis.read(bRecord, 0, RECORD_SIZE);
			System.arraycopy(bRecord, BN_NAME_OFFSET, bRecName, 0, BN_NAME_SIZE);
			if(new String(bRecName).trim().equals(searchKey)) {
				System.arraycopy(bRecord, RID_SIZE, bRecordContents, 0, RECORD_SIZE - RID_SIZE);
				System.out.println(new String(bRecordContents));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
