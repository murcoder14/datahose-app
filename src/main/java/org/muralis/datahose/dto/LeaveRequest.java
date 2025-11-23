package org.muralis.datahose.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Leave of Absence request.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LeaveRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String employeeId;
    private String leaveType;
    private String startDate;
    private String endDate;
    private String approvalStatus;
}
