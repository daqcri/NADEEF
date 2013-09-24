/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.exception;

/**
 * InvalidRuleException.
 * @author Si Yin <siyin@qf.org.qa>
 */
public class InvalidRuleException extends Exception {
    private Exception innerException;

    public InvalidRuleException(String message) {
        super(message);
    }

    public InvalidRuleException(Exception innerException) {
        this.innerException = innerException;
    }

    @Override
    public String getMessage() {
        return this.innerException.getMessage();
    }
}
