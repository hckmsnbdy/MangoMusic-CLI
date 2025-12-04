package com.mangomusic.models;

/**
 * Represents an album in the MangoMusic platform
 */
public class Album {

    private int albumId;
    private int artistId;
    private String title;
    private int releaseYear;
    private String artistName; // Denormalized for display purposes

    // Constructor with artist name
    public Album(int albumId, int artistId, String title, int releaseYear, String artistName) {
        this.albumId = albumId;
        this.artistId = artistId;
        this.title = title;
        this.releaseYear = releaseYear;
        this.artistName = artistName;
    }

    // Constructor without artist name
    public Album(int albumId, int artistId, String title, int releaseYear) {
        this(albumId, artistId, title, releaseYear, null);
    }

    // Getters
    public int getAlbumId() {
        return albumId;
    }

    public int getArtistId() {
        return artistId;
    }

    public String getTitle() {
        return title;
    }

    public int getReleaseYear() {
        return releaseYear;
    }

    public String getArtistName() {
        return artistName;
    }

    // Setters
    public void setAlbumId(int albumId) {
        this.albumId = albumId;
    }

    public void setArtistId(int artistId) {
        this.artistId = artistId;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setReleaseYear(int releaseYear) {
        this.releaseYear = releaseYear;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    @Override
    public String toString() {
        if (artistName != null) {
            return String.format("%-5d %-50s %-40s %d",
                    albumId, title, artistName, releaseYear);
        } else {
            return String.format("%-5d %-50s %d",
                    albumId, title, releaseYear);
        }
    }

    /**
     * Returns formatted header for displaying album lists
     */
    public static String getHeader() {
        return String.format("%-5s %-50s %-40s %s",
                "ID", "Title", "Artist", "Year");
    }

    /**
     * Returns formatted header for displaying albums without artist
     */
    public static String getHeaderWithoutArtist() {
        return String.format("%-5s %-50s %s",
                "ID", "Title", "Year");
    }
}