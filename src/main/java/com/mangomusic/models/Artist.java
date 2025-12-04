package com.mangomusic.models;

/**
 * Represents an artist in the MangoMusic platform
 */
public class Artist {

    private int artistId;
    private String name;
    private String primaryGenre;
    private Integer formedYear; // Using Integer to allow null

    // Constructor
    public Artist(int artistId, String name, String primaryGenre, Integer formedYear) {
        this.artistId = artistId;
        this.name = name;
        this.primaryGenre = primaryGenre;
        this.formedYear = formedYear;
    }

    // Getters
    public int getArtistId() {
        return artistId;
    }

    public String getName() {
        return name;
    }

    public String getPrimaryGenre() {
        return primaryGenre;
    }

    public Integer getFormedYear() {
        return formedYear;
    }

    // Setters
    public void setArtistId(int artistId) {
        this.artistId = artistId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPrimaryGenre(String primaryGenre) {
        this.primaryGenre = primaryGenre;
    }

    public void setFormedYear(Integer formedYear) {
        this.formedYear = formedYear;
    }

    @Override
    public String toString() {
        return String.format("%-5d %-40s %-20s %s",
                artistId,
                name,
                primaryGenre,
                formedYear != null ? formedYear : "N/A");
    }

    /**
     * Returns formatted header for displaying artist lists
     */
    public static String getHeader() {
        return String.format("%-5s %-40s %-20s %s",
                "ID", "Name", "Genre", "Formed");
    }
}