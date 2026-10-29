package com.example.reliability.model;

/**
 * failureMode: "none"      -> always succeeds
 *              "transient" -> fails the first 2 attempts, succeeds on the 3rd (retry demo)
 *              "permanent" -> always fails (goes to DLQ)
 */
public record ScheduledPostEvent(
        String eventId,
        String postId,
        String content,
        String scheduledTime,
        String failureMode) {
}