/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.util.ArrayList;

/**
 * Rule Hint : Projection
 */
public class ProjectHint extends RuleHint {
    private Cell[] attributes;

    /**
     * Parse the hint description from a string.
     * The hint description will be separated by comma.
     * @param hintDescription
     */
    @Override
    public void parse(String hintDescription) {
        String[] tokens = hintDescription.split(",");
        String defaultSchema = "public";
        ArrayList<Cell> attributeList = new ArrayList<>(tokens.length);
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
            attributeList.add(newAttribute);
        }
        attributes = attributeList.toArray(new Cell[attributeList.size()]);
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
    public ProjectHint(Cell[] attributes) {
        super();
        this.attributes = attributes;
    }

    /**
     * Getter of attributes.
     * @return Attributes.
     */
    public Cell[] getAttributes() {
        return attributes;
    }
}
