import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class hashquery implements dbimpl {

	public static void main(String[] args) {
		hashquery query = new hashquery();
		long startTime = System.currentTimeMillis();
		//query.readArguments(args);
		query.queryHash(4096, "JANSEN NEWMAN INSTITUTE");
		long endTime = System.currentTimeMillis();
		System.out.println("Load time: " + (endTime - startTime) + "ms.");
	}

	public void readArguments(String args[]) {
		if (args.length == 2) {
			if (isInteger(args[0])) {
				queryHash(Integer.parseInt(args[0]), args[1].trim());
			}
		}
	}

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

	public byte[] initializeIndex(int numOfBuckets) {

		byte[] hashIndex = new byte[numOfBuckets * BUCKET_KEYVAL_SIZE];
		return hashIndex;
	}

	public int calcNumOfBuckets(int numOfRecords, int idealOccupancy) {

		float numOfBuckets = 0;
		numOfBuckets = (float) numOfRecords * ((float) 100 / (float) idealOccupancy);
		return (int) numOfBuckets;
	}

	public int hashKey(String searchKey, int numOfBuckets) {

		int index = 0;
		int hash = 0;

		hash = searchKey.hashCode();
		index = Math.abs(hash % numOfBuckets);
		return index;
	}

	public int getBucketOffset(int bucketNum) {

		int bucketOffset = 0;

		bucketOffset = bucketNum * BUCKET_KEYVAL_SIZE;
		return bucketOffset;
	}

	public String getBucketKey(byte[] bucketKeyVal) {

		String searchKey = "";
		byte[] bRecName = new byte[BN_NAME_SIZE];

		System.arraycopy(bucketKeyVal, 0, bRecName, 0, BN_NAME_SIZE);
		searchKey = new String(bRecName);

		return searchKey;
	}

	public long getBucketVal(byte[] bucketKeyVal) {

		long heapOffset = 0;
		byte[] bHeapOffset = new byte[HEAP_FOFFSET_SIZE];

		System.arraycopy(bucketKeyVal, BN_NAME_SIZE, bHeapOffset, 0, HEAP_FOFFSET_SIZE);
		heapOffset = ByteBuffer.wrap(bHeapOffset).getLong();

		return heapOffset;
	}

	public void queryHash(int pageSize, String searchKey) {
		
		File hashFile = new File(HASH_FNAME + pageSize);
		File heapFile = new File(HEAP_FNAME + pageSize);
		int numOfRecords = countRecords(pageSize, heapFile);
		int numOfBuckets = calcNumOfBuckets(numOfRecords, IDEAL_OCCUPANCY);
		int bucketNum = hashKey(searchKey, numOfBuckets);
		
		linearProbe(searchKey, bucketNum, numOfBuckets, heapFile, hashFile);
	}

	public boolean bucketIsFull(int bucketNum, File hashFile) {

		FileInputStream hashFis = null;
		long bucketOffset = 0;
		boolean isFull = true;
		byte[] emptyBucket = new byte[BUCKET_KEYVAL_SIZE];
		try {
			bucketOffset = getBucketOffset(bucketNum);
			hashFis = new FileInputStream(hashFile);
			hashFis.skip((long) bucketOffset);
			byte[] bucketKeyVal = new byte[BUCKET_KEYVAL_SIZE];
			hashFis.read(bucketKeyVal, 0, BUCKET_KEYVAL_SIZE);
			if (Arrays.equals(emptyBucket, bucketKeyVal)) {
				isFull = false;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return isFull;
	}

	public void linearProbe(String searchKey, int bucketNum, int numOfBuckets, File heapFile, File hashFile) {

		int originalBucketNum = bucketNum;
		long bucketOffset = 0;
		FileInputStream hashFis = null;
		try {
			while (bucketIsFull(bucketNum, hashFile)) {
				byte[] bucketKeyVal = new byte[BUCKET_KEYVAL_SIZE];
				bucketOffset = getBucketOffset(bucketNum);
				hashFis = new FileInputStream(hashFile);
				hashFis.skip((long) bucketOffset);
				hashFis.read(bucketKeyVal, 0, BUCKET_KEYVAL_SIZE);
				String bucketKey = getBucketKey(bucketKeyVal);
				if (bucketKey.trim().equals(searchKey)) {
					retrieveRecord(getBucketVal(bucketKeyVal), heapFile);
				}
				if (bucketNum != numOfBuckets - 1) {
					bucketNum++;
				} else {
					bucketNum = 0;
				}
				if (bucketNum == originalBucketNum) {
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void retrieveRecord(long recordOffset, File heapFile) {

		FileInputStream heapFis;
		byte[] bRecord = new byte[RECORD_SIZE];
		byte[] bRecordContents = new byte[RECORD_SIZE - RID_SIZE];
		try {
			heapFis = new FileInputStream(heapFile);
			heapFis.skip(recordOffset);
			heapFis.read(bRecord, 0, RECORD_SIZE);
			System.arraycopy(bRecord, RID_SIZE, bRecordContents, 0, RECORD_SIZE - RID_SIZE);
			System.out.println(new String(bRecordContents));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
