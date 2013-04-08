/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * Rule Hint : Projection
 */
public class ProjectHint extends RuleHint {
    private TableAttribute[] attributes;

    /**
     * Parse the hint description from a string.
     * The hint description will be separated by comma.
     * @param hintDescription
     */
    @Override
    public void parse(String hintDescription) {
        String[] tokens = hintDescription.split(",");
        ArrayList<TableAttribute> attributeList = new ArrayList<>(tokens.length);
        for (String token : tokens) {
            if (!token.matches("^\\s*(\\w+\\.?){0,3}\\w\\s*$")) {
                throw new IllegalArgumentException("Invalid hint description " + token);
            }
            String[] attrs = token.split("\\.");
            if (attrs.length > 3 || attrs.length < 1) {
                throw new IllegalArgumentException("Invalid hint description " + token);
            }

            TableAttribute newAttribute;
            switch (attrs.length) {
                case 3:
                    newAttribute =
                            new TableAttribute(attrs[0].trim(), attrs[1].trim(), attrs[2].trim());
                    break;
                case 2:
                    newAttribute =
                            new TableAttribute(null, attrs[0].trim(), attrs[1].trim());
                    break;
                default:
                    newAttribute = new TableAttribute(null, null, attrs[0].trim());
                    break;
            }
            attributeList.add(newAttribute);
        }
        attributes = attributeList.toArray(new TableAttribute[attributeList.size()]);
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
    public ProjectHint(TableAttribute[] attributes) {
        super();
        this.attributes = attributes;
    }

    /**
     * Getter of attributes.
     * @return Attributes.
     */
    public TableAttribute[] getAttributes() {
        return attributes;
    }
}
