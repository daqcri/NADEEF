/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.exception;

import com.google.common.base.Strings;

/**
 * InvalidCleanPlanException.
 */
public class InvalidCleanPlanException extends Exception {
    private Exception innerException;
    private String message;
    public InvalidCleanPlanException(Exception ex) {
        this.innerException = ex;
    }

    public InvalidCleanPlanException(String message) {
        this.message = message;
    }

    public InvalidCleanPlanException(String message, Exception ex) {
        this.message = message;
        this.innerException = ex;
    }

    @Override
    public String getMessage() {
        if (!Strings.isNullOrEmpty(message)) {
            return message;
        }
        return this.innerException.getMessage();
    }
}
