import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class hashload implements dbimpl {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		hashload load = new hashload();
		long startTime = System.currentTimeMillis();
		load.readHeap(4096);
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
		int numOfRecords = 0;
		int recCount = 0;
		int recOffset = 0;
		int pageNum = 0;
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
		numOfBuckets = (float) numOfRecords * ((float)100/(float)idealOccupancy);
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
		byte[] emptyArray = new byte[BUCKET_KEYVAL_SIZE];
		System.arraycopy(hashIndex, bucketOffset, bucketKeyVal, 0, BUCKET_KEYVAL_SIZE);	
		if(Arrays.equals(bucketKeyVal, emptyArray)) {
			isFull = false;
		}
		return isFull;
	}

	public byte[] storeInBucket(byte[] hashIndex, byte[] bucketKeyVal, int bucketNum, int numOfBuckets) {
		
		byte[] bRecName = new byte[BN_NAME_SIZE];
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
	
	
	public void testQuery() {
		
		try {
			FileInputStream fis = null;
			int numOfRecords = countRecords(4096, new File("heap.4096"));
			int numOfBuckets = calcNumOfBuckets(numOfRecords, 70);
			int bucketNum = hashKey("Easy Ute Moves", numOfBuckets);
			while(true) {
				fis = new FileInputStream(new File("hash.4096"));
				fis.skip((long)(bucketNum * BUCKET_KEYVAL_SIZE));
				byte[] keyval = new byte[BUCKET_KEYVAL_SIZE];
				fis.read(keyval, 0, BUCKET_KEYVAL_SIZE);
				byte[] bRecName = new byte[BN_NAME_SIZE];
				System.arraycopy(keyval, 0, bRecName, 0, BN_NAME_SIZE);
				String comparison = new String(bRecName).trim();
				if(comparison.trim().equals("Easy Ute Moves")) {
					byte[] val = new byte[HEAP_FOFFSET_SIZE];
					System.arraycopy(keyval, BN_NAME_SIZE, val, 0, HEAP_FOFFSET_SIZE);
					long offset = ByteBuffer.wrap(val).getLong();
					FileInputStream fisHeap = new FileInputStream(new File("heap.4096"));
					fisHeap.skip(offset);
					byte[] record = new byte[RECORD_SIZE];
					fisHeap.read(record, 0, RECORD_SIZE);
					byte[] recordContent = new byte[RECORD_SIZE-4];
					System.arraycopy(record, 4, recordContent, 0, RECORD_SIZE-4);
					System.out.println(new String(recordContent).trim());
					break;
				} else {
					bucketNum++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
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
		/*try {
			FileInputStream fis = new FileInputStream(new File("heap.4096"));
			fis.skip(recordOffset);
			byte[] record = new byte[RECORD_SIZE];
			fis.read(record, 0, RECORD_SIZE);
			byte[] uRecName = new byte[BN_NAME_SIZE];
			System.arraycopy(record, BN_NAME_OFFSET, uRecName, 0, BN_NAME_SIZE);
			byte[] brid = new byte[RID_SIZE];
			System.arraycopy(record, 0, brid, 0, RID_SIZE);
			int xrid = ByteBuffer.wrap(brid).getInt();
			System.out.println(rid + ": " + recName.trim() + " " + xrid + ": " + new String(uRecName).trim());
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		
		System.arraycopy(bRecName, 0, bucketKeyVal, 0, BN_NAME_SIZE);
		System.arraycopy(bRecOffset, 0, bucketKeyVal, BN_NAME_SIZE, HEAP_FOFFSET_SIZE);

		return bucketKeyVal;
	}

	public void readHeap(int pageSize) {
		File heapFile = new File(HEAP_FNAME + pageSize);
		int pageCount = 0;
		int recCount = 0;
		int recordLen = 0;
		int rid = 0;
		int pageNum = 0;
		int totalCount = 0;
		boolean isNextPage = true;
		boolean isNextRecord = true;
		
		int numOfRecords = countRecords(pageSize, heapFile);
		int numOfBuckets = calcNumOfBuckets(numOfRecords, 70);
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
								int bucketNum = hashKey(recName, numOfBuckets);
								byte[] bucketKeyVal = toBucketKeyVal(recName, pageNum, rid, pageSize);
								hashIndex = storeInBucket(hashIndex, bucketKeyVal, bucketNum, numOfBuckets);
							} catch (Exception e) {
								e.printStackTrace();
							}
							recordLen += RECORD_SIZE;
							recCount++;
							totalCount++;
						}
						// if recordLen exceeds pagesize, catch this to reset to next page
					} catch (ArrayIndexOutOfBoundsException e) {
						isNextRecord = false;
						recordLen = 0;
						recCount = 0;
						rid = 0;
					}
				}
				// check to complete all pages
				if (pageNum != pageCount) {
					isNextPage = false;
				}
				pageCount++;
			}
		} catch (FileNotFoundException e) {
			System.out.println("File: " + HEAP_FNAME + pageSize + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}
		writeToFile(hashIndex, pageSize);
		long timenow = System.currentTimeMillis();
		testQuery();
		long timeend = System.currentTimeMillis();
		System.out.println(timeend - timenow + "ms");
	}

	public void writeToFile(byte[] hashIndex, int pageSize) {

		FileOutputStream fos;
		File hashFile;

		try {
			hashFile = new File("hash." + 4096);
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
