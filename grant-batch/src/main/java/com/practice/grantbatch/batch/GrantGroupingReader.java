package com.practice.grantbatch.batch;

import com.practice.grantbatch.model.Models.GrantChange;
import com.practice.grantbatch.model.Models.GrantGroup;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;

import java.util.ArrayList;
import java.util.List;

/** Wraps a row-level reader and emits one GrantGroup per run of rows sharing a grant_id. */
public class GrantGroupingReader implements ItemStreamReader<GrantGroup> {

    private final SingleItemPeekableItemReader<GrantChange> delegate = new SingleItemPeekableItemReader<>();

    public GrantGroupingReader(ItemStreamReader<GrantChange> rowReader) {
        delegate.setDelegate(rowReader);
    }

    @Override
    public GrantGroup read() throws Exception {
        GrantChange first = delegate.read();
        if (first == null) return null;
        List<GrantChange> changes = new ArrayList<>();
        changes.add(first);
        GrantChange next;
        while ((next = delegate.peek()) != null && next.grantId() == first.grantId()) {
            changes.add(delegate.read());
        }
        return new GrantGroup(first.grantId(), changes);
    }

    @Override public void open(ExecutionContext ctx) throws ItemStreamException { delegate.open(ctx); }
    @Override public void update(ExecutionContext ctx) throws ItemStreamException { delegate.update(ctx); }
    @Override public void close() throws ItemStreamException { delegate.close(); }
}
