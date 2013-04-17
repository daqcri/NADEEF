/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule Hint : Projection
 */
public class ProjectHint extends RuleHint {
    private List<Cell> attributes;

    /**
     * Parse the hint description from a string.
     * The hint description will be separated by comma.
     * @param hintDescription
     */
    @Override
    public void parse(String hintDescription) {
        String[] tokens = hintDescription.split(",");
        String defaultSchema = "public";
        attributes = new ArrayList(tokens.length);
        for (String token : tokens) {
            if (!token.matches("^\\s*(\\w+\\.?){0,3}\\w\\s*$")) {
                throw new IllegalArgumentException("Invalid hint description " + token);
            }
            String[] attrs = token.split("\\.");
            if (attrs.length > 3 || attrs.length < 1) {
                throw new IllegalArgumentException("Invalid hint description " + token);
            }

            Cell newAttribute;
            switch (attrs.length) {
                case 3:
                    newAttribute =
                            new Cell(attrs[0].trim(), attrs[1].trim(), attrs[2].trim());
                    break;
                case 2:
                    newAttribute =
                            new Cell(defaultSchema, attrs[0].trim(), attrs[1].trim());
                    break;
                default:
                    newAttribute = new Cell(defaultSchema, null, attrs[0].trim());
                    break;
            }
            attributes.add(newAttribute);
        }
    }

    /**
     * Constructor.
     * @param hintDescription hint description in string.
     */
    public ProjectHint(String hintDescription) {
        super(hintDescription);
    }

    /**
     * Constructor.
     * @param attributes
     */
    public ProjectHint(List<Cell> attributes) {
        super();
        this.attributes = attributes;
    }

    /**
     * Getter of attributes.
     * @return Attributes.
     */
    public List<Cell> getAttributes() {
        return attributes;
    }
}