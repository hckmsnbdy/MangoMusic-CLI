package com.mangomusic.data;

import com.mangomusic.models.Album;
import com.mangomusic.models.ReportResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class AlbumDao {

    private final DataManager dataManager;
    private final ArtistDao artistDao;

    public AlbumDao(DataManager dataManager) {
        this.dataManager = dataManager;
        this.artistDao = new ArtistDao(dataManager);
    }

    public List<Album> getAlbumsByArtist(int artistId) {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE al.artist_id = ? " +
                "ORDER BY al.release_year DESC";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query)) {

                statement.setInt(1, artistId);

                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        int albumId = results.getInt("album_id");
                        int artId = results.getInt("artist_id");
                        String title = results.getString("title");
                        int releaseYear = results.getInt("release_year");
                        String artistName = results.getString("artist_name");

                        albums.add(new Album(albumId, artId, title, releaseYear, artistName));
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error getting albums for artist: " + e.getMessage());
            e.printStackTrace();
        }

        return albums;
    }

    public List<Album> getAlbumsByGenre(String genre) {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE ar.primary_genre = ? " +
                "ORDER BY al.title";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query)) {

                statement.setString(1,genre);

                try(ResultSet results = statement.executeQuery()){

                    while (results.next()) {
                        int albumId = results.getInt("album_id");
                        int artistId = results.getInt("artist_id");
                        String title = results.getString("title");
                        int releaseYear = results.getInt("release_year");
                        String artistName = results.getString("artist_name");

                        albums.add(new Album(albumId, artistId, title, releaseYear, artistName));
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error getting albums by genre: " + e.getMessage());
            e.printStackTrace();
        }

        return albums;
    }
    public List<ReportResult> getMostPlayedAlbumsByGenre() {
        List<ReportResult> results = new ArrayList<>();

        List<String> genres = artistDao.getAllGenres();

        String sql =
                "SELECT al.title, ar.name AS artist_name, COUNT(*) AS play_count " +
                        "FROM album_plays ap " +
                        "JOIN albums al ON (ap.album_id = al.album_id) " +
                        "JOIN artists ar ON (al.artist_id = ar.artist_id) " +
                        "WHERE ar.primary_genre = ? " +
                        "GROUP BY al.album_id, al.title, ar.name " +
                        "ORDER BY play_count DESC " +
                        "LIMIT 5";

        try {
            Connection connection = dataManager.getConnection();

            for (String genre : genres) {

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, genre);

                    try (ResultSet rs = statement.executeQuery()) {

                        int rank = 1;

                        while (rs.next()) {

                            String title = rs.getString("title");
                            String artistName = rs.getString("artist_name");
                            long playCount = rs.getLong("play_count");

                            ReportResult row = new ReportResult();
                            row.addColumn("genre", genre);
                            row.addColumn("album_title", title);
                            row.addColumn("artist_name", artistName);
                            row.addColumn("play_count", playCount);
                            row.addColumn("genre_rank", rank);

                            results.add(row);
                            rank++;
                        }
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error getting most played albums by genre: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }


    public List<Album> searchAlbums(String searchTerm) {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE al.title LIKE ? " +
                "ORDER BY al.title";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query)) {

                statement.setString(1, "%" + searchTerm + "%");

                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        int albumId = results.getInt("album_id");
                        int artistId = results.getInt("artist_id");
                        String title = results.getString("title");
                        int releaseYear = results.getInt("release_year");
                        String artistName = results.getString("artist_name");

                        albums.add(new Album(albumId, artistId, title, releaseYear, artistName));
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error searching for albums: " + e.getMessage());
            e.printStackTrace();
        }

        return albums;
    }
}