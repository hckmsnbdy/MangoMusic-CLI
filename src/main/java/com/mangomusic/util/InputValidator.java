package com.mangomusic.util;

import java.util.Scanner;

/**
 * Utility class for validating and parsing user input
 */
public class InputValidator {

    private static final Scanner scanner = new Scanner(System.in);

    /**
     * Gets a valid integer input from user within a range
     * @param prompt The message to display
     * @param min Minimum valid value
     * @param max Maximum valid value
     * @return Valid integer input
     */
    public static int getIntInRange(String prompt, int min, int max) {
        while (true) {
            System.out.print(prompt);
            try {
                String input = scanner.nextLine().trim();
                int value = Integer.parseInt(input);

                if (value >= min && value <= max) {
                    return value;
                } else {
                    System.out.println("Please enter a number between " + min + " and " + max);
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a valid number.");
            }
        }
    }

    /**
     * Gets a non-empty string from user
     * @param prompt The message to display
     * @return Non-empty string input
     */
    public static String getNonEmptyString(String prompt) {
        while (true) {
            System.out.print(prompt);
            String input = scanner.nextLine().trim();

            if (!input.isEmpty()) {
                return input;
            } else {
                System.out.println("Input cannot be empty. Please try again.");
            }
        }
    }

    /**
     * Waits for user to press Enter
     */
    public static void pressEnterToContinue() {
        System.out.print("\nPress Enter to continue...");
        scanner.nextLine();
    }

    /**
     * Clears the console (works in most terminals)
     */
    public static void clearScreen() {
        try {
            if (System.getProperty("os.name").contains("Windows")) {
                new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
            } else {
                System.out.print("\033[H\033[2J");
                System.out.flush();
            }
        } catch (Exception e) {
            // If clear doesn't work, just print blank lines
            for (int i = 0; i < 50; i++) {
                System.out.println();
            }
        }
    }
}