package com.daasyyds.flink.sql.analyzer.ability.setting;

import com.daasyyds.flink.sql.analyzer.ability.result.IdentifierLike;

import java.net.URI;
import java.util.Collection;
import java.util.List;

public class ReplaceTableSetting extends ParseSetting {
    private Collection<ReplacePair> replace;

    private ReplaceTableSetting(){
    }

    public ReplaceTableSetting(Collection<ReplacePair> replace) {
        assert replace != null && !replace.isEmpty();
        this.replace = replace;
    }

    public ReplaceTableSetting(List<URI> dependencies, String initCatalog, String initDatabase, Collection<ReplacePair> replace) {
        super(dependencies, initCatalog, initDatabase);
        assert replace != null && !replace.isEmpty();
        this.replace = replace;
    }

    public Collection<ReplacePair> getReplace() {
        return replace;
    }

    public static class ReplacePair {
        private IdentifierLike from;
        private IdentifierLike to;

        private ReplacePair(IdentifierLike from, IdentifierLike to) {
            this.from = from;
            this.to = to;
        }

        public static ReplacePair of(IdentifierLike from, IdentifierLike to) {
            return new ReplacePair(from, to);
        }

        public IdentifierLike getFrom() {
            return from;
        }

        public IdentifierLike getTo() {
            return to;
        }
    }
}
