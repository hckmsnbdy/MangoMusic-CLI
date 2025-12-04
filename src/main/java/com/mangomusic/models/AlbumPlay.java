package com.mangomusic.models;

import java.time.LocalDateTime;

/**
 * Represents a single album play/listen event
 */
public class AlbumPlay {

    private long playId;
    private int userId;
    private int albumId;
    private LocalDateTime playedAt;
    private boolean completed;

    // Denormalized fields for display
    private String albumTitle;
    private String artistName;

    // Constructor with denormalized fields
    public AlbumPlay(long playId, int userId, int albumId, LocalDateTime playedAt,
                     boolean completed, String albumTitle, String artistName) {
        this.playId = playId;
        this.userId = userId;
        this.albumId = albumId;
        this.playedAt = playedAt;
        this.completed = completed;
        this.albumTitle = albumTitle;
        this.artistName = artistName;
    }

    // Constructor without denormalized fields
    public AlbumPlay(long playId, int userId, int albumId, LocalDateTime playedAt, boolean completed) {
        this(playId, userId, albumId, playedAt, completed, null, null);
    }

    // Getters
    public long getPlayId() {
        return playId;
    }

    public int getUserId() {
        return userId;
    }

    public int getAlbumId() {
        return albumId;
    }

    public LocalDateTime getPlayedAt() {
        return playedAt;
    }

    public boolean isCompleted() {
        return completed;
    }

    public String getAlbumTitle() {
        return albumTitle;
    }

    public String getArtistName() {
        return artistName;
    }

    // Setters
    public void setPlayId(long playId) {
        this.playId = playId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public void setAlbumId(int albumId) {
        this.albumId = albumId;
    }

    public void setPlayedAt(LocalDateTime playedAt) {
        this.playedAt = playedAt;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public void setAlbumTitle(String albumTitle) {
        this.albumTitle = albumTitle;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    @Override
    public String toString() {
        String completedStr = completed ? "✓" : "✗";
        return String.format("%-50s %-40s %-20s %s",
                albumTitle != null ? albumTitle : "Album #" + albumId,
                artistName != null ? artistName : "",
                playedAt.toLocalDate(),
                completedStr);
    }

    /**
     * Returns formatted header for displaying play history
     */
    public static String getHeader() {
        return String.format("%-50s %-40s %-20s %s",
                "Album", "Artist", "Played At", "Completed");
    }
}