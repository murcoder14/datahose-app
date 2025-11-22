package org.muralis.datahose.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * Generic file processing metadata.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class FileMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private String fileName;
    private String type;
    private String format;
}
