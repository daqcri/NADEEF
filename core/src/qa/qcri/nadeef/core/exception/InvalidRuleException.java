/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.exception;

/**
 * InvalidRuleException.
 */
public class InvalidRuleException extends Exception {
    private Exception innerException;

    public InvalidRuleException(String message) {
        super(message);
    }

    public InvalidRuleException(Exception innerException) {
        this.innerException = innerException;
    }
}
