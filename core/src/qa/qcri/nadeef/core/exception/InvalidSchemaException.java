/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.exception;

/**
 */
public class InvalidSchemaException extends Exception {
    private Exception innerException;
    public InvalidSchemaException(Exception innerException) {
        super(innerException.getMessage());
        this.innerException = innerException;
    }

    @Override
    public String getMessage() {
        return this.innerException.getMessage();
    }
}
