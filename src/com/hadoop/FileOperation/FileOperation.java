package com.hadoop.FileOperation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileOperation {

	public void FileRelative(Path input, Path input_tmp, FileSystem fs) throws IOException {
		// public static void main(String[] args) throws IOException{
		// 需要讀取的目錄
		// conf.set("fs.default.name", "hdfs://localhost:9000");
		// Configuration conf = new Configuration();
		// FileSystem fs = FileSystem.get(conf);
		// Path path = new Path("/input");
		// 合併的文件輸出的目錄
		System.out.println("begin to merge file");
		fs.mkdirs(input_tmp);
		FileStatus[] fileStatus = fs.listStatus(input);
		// 開始讀取文件
		int i;
		byte[] bytes = new byte[64 * 1024 * 1024];
		for (i = 0; i < fileStatus.length; i++) {
			if (fileStatus[i].isDir()) {
				System.out.print("---begin merge directory---" + fileStatus[i].getPath().getName());
				Path p_dir = new Path(fileStatus[i].getPath().toString());
				FileStatus[] fileStatus2 = fs.listStatus(p_dir);
				Path file = new Path(
						input_tmp.getName() + java.io.File.separator + fileStatus[i].getPath().getName() + ".txt");
				FSDataOutputStream file_stream = fs.create(file);
				for (int j = 0; j < fileStatus2.length; j++) {
					Path p_file = new Path(fileStatus2[j].getPath().toString());
					file_stream.writeBytes(readFile(p_file, fs));
				}
				file_stream.close();
				System.out.println("---finish merge directory---" + fileStatus[i].getPath().getName());
			} else {
				System.out.println("something wrong!");
			}
		}
		if (i == fileStatus.length) {
			System.out.println("---------------temp Input File DONE");
		}
	}

	public static String readFile(Path path, FileSystem fs) throws IOException {
		FSDataInputStream is = fs.open(path);
		FileStatus status = fs.getFileStatus(path);
		byte[] buffer = new byte[Integer.parseInt(String.valueOf(status.getLen()))];
		is.read(buffer);
		is.close();
		return new String(buffer);// 若是buffer.toString()則不能正常的轉換
	}

}
