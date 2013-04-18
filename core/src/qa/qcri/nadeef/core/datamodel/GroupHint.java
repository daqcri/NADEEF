/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * Group Hint.
 */
public class GroupHint extends RuleHint {
    private Cell cell;

    /**
     * Constructor.
     * @param cell input cell.
     */
    public GroupHint(Cell cell) {
        this.cell = cell;
    }

    /**
     * Parse the hint description from a string.
     *
     * @param hintDescription
     */
    @Override
    public void parse(String hintDescription) {
        hintDescription = hintDescription.trim();
        if (!hintDescription.matches("^\\s*(\\w+\\.?){0,3}\\w\\s*$")) {
            throw new IllegalArgumentException("Invalid hint description " + hintDescription);
        }

        String[] attrs = hintDescription.split("\\.");
        if (attrs.length > 3 || attrs.length < 1) {
            throw new IllegalArgumentException("Invalid hint description " + hintDescription);
        }

        String defaultSchema = "public";
        switch (attrs.length) {
            case 3:
                cell = new Cell(attrs[0].trim(), attrs[1].trim(), attrs[2].trim());
                break;
            case 2:
                cell = new Cell(defaultSchema, attrs[0].trim(), attrs[1].trim());
                break;
            default:
                cell = new Cell(defaultSchema, null, attrs[0].trim());
                break;
        }
    }
}
