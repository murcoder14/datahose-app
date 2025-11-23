package org.muralis.datahose.dto;

/**
 * Enum representing different message types supported by the streaming application.
 * Currently supports Claims and Leave of Absence requests, with Unknown for unrecognized messages.
 */
public enum MessageType {
    CLAIMS,
    LEAVE_OF_ABSENCE,
    UNKNOWN
}
