package qa.qcri.nadeef.web.sql;

import com.google.common.base.Preconditions;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import java.io.File;

/**
 * MySQL Dialect.
 */
public class MySQLDialect extends SQLDialectBase {
    private static STGroupFile template =
        new STGroupFile(
            "qa*qcri*nadeef*web*sql*template*MySQLTemplate.stg".replace(
                "*", "/"
            ), '$', '$');

    /**
     * {@inheritDoc}
     */
    @Override
    public STGroupFile getTemplate() {
        return template;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String installRule() {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST instance = template.getInstanceOf("InstallRule");
        instance.add("name", "RULE");
        return instance.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String installRuleType() {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST instance = template.getInstanceOf("InstallRuleType");
        instance.add("name", "RULETYPE");
        return instance.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String insertRule(String type, String code, String table1, String table2, String name) {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST instance = template.getInstanceOf("InsertRule");
        instance.add("name", name);
        instance.add("type", type);
        instance.add("code", code);
        instance.add("table1", table1);
        instance.add("table2", table2);
        return instance.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String queryTopK(int k) {
        return "select tupleid, count(distinct(vid)) as count from VIOLATION group by tupleid " +
            "order by count desc LIMIT " + k;
    }
}
