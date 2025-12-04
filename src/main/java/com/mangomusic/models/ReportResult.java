package com.mangomusic.models;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Generic class for holding report results
 * Uses a Map to store column name -> value pairs for flexibility
 */
public class ReportResult {

    private Map<String, Object> data;

    public ReportResult() {
        this.data = new LinkedHashMap<>(); // Maintains insertion order
    }

    /**
     * Adds a column value to the result
     */
    public void addColumn(String columnName, Object value) {
        data.put(columnName, value);
    }

    /**
     * Gets a value by column name
     */
    public Object get(String columnName) {
        return data.get(columnName);
    }

    /**
     * Gets a string value by column name
     */
    public String getString(String columnName) {
        Object value = data.get(columnName);
        return value != null ? value.toString() : "";
    }

    /**
     * Gets an integer value by column name
     */
    public Integer getInt(String columnName) {
        Object value = data.get(columnName);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }

    /**
     * Gets a double value by column name
     */
    public Double getDouble(String columnName) {
        Object value = data.get(columnName);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return null;
    }

    /**
     * Returns all column names in order
     */
    public String[] getColumnNames() {
        return data.keySet().toArray(new String[0]);
    }

    /**
     * Returns all values in order
     */
    public Object[] getValues() {
        return data.values().toArray();
    }

    /**
     * Returns the underlying data map
     */
    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        return sb.toString();
    }
}