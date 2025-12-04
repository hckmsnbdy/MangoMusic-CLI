package com.mangomusic.data;

import com.mangomusic.models.Artist;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ArtistDao {

    private final DataManager dataManager;

    public ArtistDao(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public List<Artist> searchArtists(String searchTerm) {
        List<Artist> artists = new ArrayList<>();
        String query = "SELECT artist_id, name, primary_genre, formed_year " +
                "FROM artists " +
                "WHERE name LIKE ? " +
                "ORDER BY name";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query)) {

                statement.setString(1, "%" + searchTerm + "%");

                try(ResultSet results = statement.executeQuery()){



                    while (results.next()) {
                        int artistId = results.getInt("artist_id");
                        String name = results.getString("name");
                        String genre = results.getString("primary_genre");
                        Integer formedYear = results.getObject("formed_year", Integer.class);

                        artists.add(new Artist(artistId, name, genre, formedYear));
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error searching for artists: " + e.getMessage());
            e.printStackTrace();
        }

        return artists;
    }

    public List<String> getAllGenres() {
        List<String> genres = new ArrayList<>();
        String query = "SELECT DISTINCT primary_genre FROM artists ORDER BY primary_genre";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet results = statement.executeQuery()) {

                while (results.next()) {
                    genres.add(results.getString("primary_genre"));
                }
            }

        } catch (SQLException e) {
            System.err.println("Error getting genres: " + e.getMessage());
            e.printStackTrace();
        }

        return genres;
    }
}