package com.beans;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Scanner;

public class Main {

    private static Logger log = Logger.getLogger("file");
    static Connection dbConnection;

    private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_CONNECTION = "jdbc:mysql://127.0.0.1:3306/emails";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";

    public static void main(String[] argv) {
        String path = "C:\\pathToFolderWithFile";
        int keyNumber = 0;

        dbConnection = getDBConnection();
        saveEmails(path, keyNumber);
        closeConnectionQuietly(dbConnection);
    }

    private static void closeConnectionQuietly(Connection dbConnection) {
        if (dbConnection != null) {
            try {
                dbConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                log.error("closing connection exception", e);
            }
        }
    }

    private static void insertRecordIntoDbUserTable(String key) {
        Statement statement = null;
        String insertTableSQL = "insert into emails (email) values ('" + key + "')";
        try {
            statement = dbConnection.createStatement();
            System.out.println(insertTableSQL);
            statement.executeUpdate(insertTableSQL);
        } catch (MySQLIntegrityConstraintViolationException e) {
            logConstraintViolationException(e);
        } catch (SQLException e) {
            logSqlException(e, insertTableSQL);
        } finally {
            closeStatementQuietly(statement);
        }
    }

    private static void logSqlException(SQLException e, String insertTableSQL) {
        e.printStackTrace();
        log.error("error while executing query: " + insertTableSQL, e);
    }

    private static void logConstraintViolationException(MySQLIntegrityConstraintViolationException e) {
        String message = e.getMessage();
        String email = message.replace("for key 'email'", "").replace("Duplicate entry ", "");
        log.error("dublicated email " + email);
    }

    private static void closeStatementQuietly(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
                log.error("closing statement exception", e);
            }
        }
    }

    private static Connection getDBConnection() {
        Connection dbConnection = null;
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error("load driver error", e);
            try {
                log.info("waiting to load driver...");
                Thread.currentThread().wait(2 * 60 * 1000);
                getDBConnection();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                log.error("interrupted exception while waiting for driver", ex);
            }
        }

        try {
            dbConnection = DriverManager.getConnection(
                    DB_CONNECTION, DB_USER, DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("get db connection error", e);
            try {
                log.info("waiting to get connection...");
                Thread.currentThread().wait(20 * 60 * 1000);
                getDBConnection();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                log.error("interrupted exception while waiting for connection", ex);
            }
        }
        return dbConnection;
    }

    public static void saveEmails(String path, int keyNumber) {
        File fdirectory = new File(path);
        if (fdirectory.isFile()) {
            log.error("path: " + path + " is not directory");
            return;
        }

        File[] list = fdirectory.listFiles();
        for (File f : list) {
            saveEmail(f, keyNumber, path);
        }
    }

    private static void saveEmail(File f, int keyNumber, String path) {
        Scanner sc;
        try {
            log.info("checking file: " + f.getName());
            sc = new Scanner(f);
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String keys[] = line.split(":");
                EmailValidator manager = new EmailValidator();
                if (manager.validate(keys[keyNumber])) {
                    String email = keys[keyNumber].trim();
                    insertRecordIntoDbUserTable(email);
                }
            }
            sc.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("file not found: " + path, e);
        }
    }
}
