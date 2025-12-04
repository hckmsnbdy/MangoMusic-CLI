package com.mangomusic.models;

import java.time.LocalDate;

/**
 * Represents a user in the MangoMusic platform
 */
public class User {

    private int userId;
    private String username;
    private String email;
    private LocalDate signupDate;
    private String subscriptionType;
    private String country;

    // Constructor
    public User(int userId, String username, String email, LocalDate signupDate,
                String subscriptionType, String country) {
        this.userId = userId;
        this.username = username;
        this.email = email;
        this.signupDate = signupDate;
        this.subscriptionType = subscriptionType;
        this.country = country;
    }

    // Getters
    public int getUserId() {
        return userId;
    }

    public String getUsername() {
        return username;
    }

    public String getEmail() {
        return email;
    }

    public LocalDate getSignupDate() {
        return signupDate;
    }

    public String getSubscriptionType() {
        return subscriptionType;
    }

    public String getCountry() {
        return country;
    }

    // Setters
    public void setUserId(int userId) {
        this.userId = userId;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setSignupDate(LocalDate signupDate) {
        this.signupDate = signupDate;
    }

    public void setSubscriptionType(String subscriptionType) {
        this.subscriptionType = subscriptionType;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String toString() {
        return String.format("%-5d %-20s %-30s %-12s %-10s %s",
                userId, username, email, signupDate, subscriptionType, country);
    }

    /**
     * Returns formatted header for displaying user lists
     */
    public static String getHeader() {
        return String.format("%-5s %-20s %-30s %-12s %-10s %s",
                "ID", "Username", "Email", "Signup Date", "Type", "Country");
    }
}