package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.ArrayList;

public class CommitLog {
    private String logFile;
    private int sequence=-1;
    private ReadWriteLock rwlock = new ReentrantReadWriteLock();

    public CommitLog(String logFile) {
        this.logFile = logFile;
    }

    public void log(String msg) {
        rwlock.writeLock().lock();
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)))) {
            out.println(msg);
            sequence += 1;
        } catch (IOException e) {
            System.err.println(e);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    public synchronized List<String> readLines(int startSeq) {
        BufferedReader reader;
        List<String> output = new ArrayList<String>();

        rwlock.readLock().lock();
		try {
            File f = new File(logFile);
            
            if (f.exists() && !f.isDirectory()) {
                int s = 0;
                reader = new BufferedReader(new FileReader(logFile));
                while(true) {
                    String line = reader.readLine();
                    if (line == null) break;
                    if (s >= startSeq) {
                        output.add(line);
                    }
                    s += 1;
                }
                reader.close();
            }
            
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
            rwlock.readLock().unlock();
        }

        return output;
    }

    public int getSequence() {
        return sequence;
    }

    public void deleteLog() {
        try {
            File myObj = new File(logFile); 
            if (myObj.delete()) { 
                System.out.println("Deleted the file: " + myObj.getName());
            } else {
                System.out.println("Failed to delete the file.");
            } 
        } catch (Exception e) {
			e.printStackTrace();
		}
    }
}
