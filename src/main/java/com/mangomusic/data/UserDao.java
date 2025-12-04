package com.mangomusic.data;

import com.mangomusic.models.User;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class UserDao {

    private final DataManager dataManager;

    public UserDao(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public List<User> searchUsers(String username) {
        List<User> users = new ArrayList<>();
        String query = "SELECT user_id, username, email, signup_date, subscription_type, country " +
                "FROM users " +
                "WHERE username LIKE ? OR email LIKE ? " +
                "ORDER BY username";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query)) {

                statement.setString(1, "%" + username + "%");
                statement.setString(1, "%" + username + "%");

                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        int userId = results.getInt("user_id");
                        String user = results.getString("username");
                        String email = results.getString("email");
                        LocalDate signupDate = results.getDate("signup_date").toLocalDate();
                        String subscriptionType = results.getString("subscription_type");
                        String country = results.getString("country");

                        users.add(new User(userId, user, email, signupDate, subscriptionType, country));
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error searching for users: " + e.getMessage());
            e.printStackTrace();
        }

        return users;
    }
}