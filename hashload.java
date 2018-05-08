import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class hashload implements dbimpl {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		hashload load = new hashload();
		long startTime = System.currentTimeMillis();
		load.readArguments(new String[]{"4096"});
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
		int recordsPerPage = pageSize/RECORD_SIZE;
		long heapFileSize = heapFile.length();
		long offset = heapFileSize - pageSize;
		boolean hasNextRecord = true;
		try {
			byte[] bLastPage = new byte[pageSize];
			byte[] bPageNum = new byte[EOF_PAGENUM_SIZE];
			fis = new FileInputStream(heapFile);
			fis.skip(offset);
			fis.read(bLastPage, 0, pageSize);
			System.arraycopy(bLastPage, pageSize-EOF_PAGENUM_SIZE, bPageNum, 0, EOF_PAGENUM_SIZE);
			pageNum = ByteBuffer.wrap(bPageNum).getInt();
			while(hasNextRecord) {
				try {
					byte[] bRecord = new byte[RECORD_SIZE];
					byte[] bRid = new byte[RID_SIZE];
					int rid = 0;
					System.arraycopy(bLastPage, recOffset, bRecord, 0, RECORD_SIZE);
					System.arraycopy(bRecord, 0, bRid, 0, RID_SIZE);
					rid = ByteBuffer.wrap(bRid).getInt();
					if(rid < recCount) {
						hasNextRecord = false;
					} else {
						recOffset += RECORD_SIZE;
						recCount++;
					}
				} catch(ArrayIndexOutOfBoundsException e) {
					hasNextRecord = false;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		numOfRecords = (pageNum * recordsPerPage) + recCount;
		return numOfRecords;
	}
	
	public byte[] intiliazeIndex(int numOfRecords) {
		
		int numOfBuckets = 0;
		
		// try to get 70% occupancy rate;
		numOfBuckets = (numOfRecords/7) * 100;
		
		return null;
	}
	
	public void readHeap(int pageSize) {
		File heapFile = new File(HEAP_FNAME + pageSize);
		int numOfRecords = countRecords(pageSize, heapFile);
		int pageCount = 0;
		int recCount = 0;
		int recordLen = 0;
		int rid = 0;
		int pageNum = 0;
		int hashCode = 0;
		int bucketNumber = 0;
		String recordName = "";
		boolean isNextPage = true;
		boolean isNextRecord = true;
		
		numOfRecords = countRecords(pageSize, heapFile);
		
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
						System.arraycopy(bRecord, BN_NAME_OFFSET, bRecName, 0, BN_NAME_SIZE);
						System.arraycopy(bRecord, 0, bRid, 0, RID_SIZE);
						rid = ByteBuffer.wrap(bRid).getInt();
						recName = new String(bRecName).trim();
						if (rid < recCount) {
							isNextRecord = false;
						} else {
							//printRecord(bRecord, name);
							recordLen += RECORD_SIZE;
						}
						recCount++;
						// if recordLen exceeds pageSize, catch this to reset to next page
					} catch (ArrayIndexOutOfBoundsException e) {
						isNextRecord = false;
						recordLen = 0;
						recCount = 0;
						rid = 0;
					}
				}
				// check to complete all pages
				if (ByteBuffer.wrap(bPageNum).getInt() != pageCount) {
					isNextPage = false;
				}
				pageCount++;
			}
		} catch (FileNotFoundException e) {
			System.out.println("File: " + HEAP_FNAME + pageSize + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
