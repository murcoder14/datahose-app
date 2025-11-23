package org.muralis.datahose.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Claims processing result.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Claim implements Serializable {
    private static final long serialVersionUID = 1L;

    private String claimId;
    private String status;
    private double amount;
    private String processedAt;
}
