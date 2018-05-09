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

	public void readArguments(String args[]) {
		if (args.length == 1) {
			if (isInteger(args[0])) {
				readHeap(Integer.parseInt(args[0]));
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

	public boolean isBucketFull(byte[] hashIndex, int bucketOffset) {

		boolean isFull = true;
		byte[] bucketKeyVal = new byte[BUCKET_KEYVAL_SIZE];
		byte[] emptyBucket = new byte[BUCKET_KEYVAL_SIZE];
		System.arraycopy(hashIndex, bucketOffset, bucketKeyVal, 0, BUCKET_KEYVAL_SIZE);
		if (Arrays.equals(bucketKeyVal, emptyBucket)) {
			isFull = false;
		}
		return isFull;
	}

	public byte[] storeInBucket(byte[] hashIndex, String recName, int pageNum, int rid, int pageSize,
			int numOfBuckets) {

		byte[] bRecName = new byte[BN_NAME_SIZE];
		int bucketNum = hashKey(recName, numOfBuckets);
		byte[] bucketKeyVal = toBucketKeyVal(recName, pageNum, rid, pageSize);
		System.arraycopy(bucketKeyVal, 0, bRecName, 0, BN_NAME_SIZE);
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
				System.arraycopy(bucketKeyVal, 0, hashIndex, bucketOffset, BUCKET_KEYVAL_SIZE);
				stored = true;
			}
		}
		return hashIndex;
	}

	public void checkHashRecords(int pageSize, int numOfRecords, int numOfBuckets) {

		FileInputStream fis = null;
		int recordCount = 0;
		boolean eof = false;
		byte[] emptybucket = new byte[BUCKET_KEYVAL_SIZE];
		byte[] hashIndex = new byte[numOfBuckets * BUCKET_KEYVAL_SIZE];
		try {
			fis = new FileInputStream(new File(HASH_FNAME + pageSize));
			byte[] keyval = new byte[BUCKET_KEYVAL_SIZE];
			fis.read(hashIndex, 0, numOfBuckets * BUCKET_KEYVAL_SIZE);
			int offset = 0;
			while (!eof) {
				System.arraycopy(hashIndex, offset, keyval, 0, BUCKET_KEYVAL_SIZE);
				if (!Arrays.equals(keyval, emptybucket)) {
					recordCount++;
				}
				offset += BUCKET_KEYVAL_SIZE;
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			eof = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(recordCount + "/" + numOfRecords + " in the hashIndex.");
	}

	public byte[] toBucketKeyVal(String recName, int pageNum, int rid, int pageSize) {

		byte[] bucketKeyVal = new byte[BUCKET_KEYVAL_SIZE];
		byte[] bRecName = new byte[BN_NAME_SIZE];
		byte[] bRecOffset = new byte[HEAP_FOFFSET_SIZE];
		byte[] recNameToB = null;
		long recordOffset = (pageNum * pageSize) + (rid * RECORD_SIZE);

		bRecOffset = ByteBuffer.allocate(HEAP_FOFFSET_SIZE).putLong(recordOffset).array();
		recNameToB = recName.getBytes();
		System.arraycopy(recNameToB, 0, bRecName, 0, recNameToB.length);

		System.arraycopy(bRecName, 0, bucketKeyVal, 0, BN_NAME_SIZE);
		System.arraycopy(bRecOffset, 0, bucketKeyVal, BN_NAME_SIZE, HEAP_FOFFSET_SIZE);

		return bucketKeyVal;
	}

	public void readHeap(int pageSize) {
		File heapFile = new File(HEAP_FNAME + pageSize);
		int pageCount, recCount, recordLen, rid, pageNum;
		pageCount = recCount = recordLen = rid = pageNum = 0;
		boolean isNextPage = true;
		boolean isNextRecord = true;

		int numOfRecords = countRecords(pageSize, heapFile);
		int numOfBuckets = calcNumOfBuckets(numOfRecords, IDEAL_OCCUPANCY);
		byte[] hashIndex = initializeIndex(numOfBuckets);

		try {
			FileInputStream fis = new FileInputStream(heapFile);
			// reading page by page
			while (isNextPage) {
				byte[] bPage = new byte[pageSize];
				byte[] bPageNum = new byte[EOF_PAGENUM_SIZE];
				fis.read(bPage, 0, pageSize);
				System.arraycopy(bPage, bPage.length - EOF_PAGENUM_SIZE, bPageNum, 0, EOF_PAGENUM_SIZE);
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
						System.arraycopy(bRecord, 0, bRid, 0, RID_SIZE);
						System.arraycopy(bRecord, BN_NAME_OFFSET, bRecName, 0, BN_NAME_SIZE);
						rid = ByteBuffer.wrap(bRid).getInt();
						recName = new String(bRecName).trim();
						if (rid != recCount) {
							isNextRecord = false;
						} else {
							try {
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
			hashIndexToFile(hashIndex, pageSize);
			// Uncomment to check if all records have been loaded into hashFile
			// properly.
			// checkHashRecords(pageSize, numOfRecords, numOfBuckets);
		} catch (FileNotFoundException e) {
			System.out.println("File: " + HEAP_FNAME + pageSize + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

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