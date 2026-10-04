package com.practice.grantbatch.model;

import java.util.List;

public final class Models {
    private Models() {}

    /** One row of grant_change. */
    public record GrantChange(long id, long grantId, String changeType, String newValue) {}

    /** What the reader emits: every pending change for a single grant. */
    public record GrantGroup(long grantId, List<GrantChange> changes) {}

    /** What the processor emits. error != null means the egc call failed after retries. */
    public record AwardResult(long grantId, String awardType, long shares, long amountCents,
                              String currency, String error, GrantGroup group) {
        public static AwardResult failed(GrantGroup group, String error) {
            return new AwardResult(group.grantId(), null, 0, 0, null, error, group);
        }
    }
}
