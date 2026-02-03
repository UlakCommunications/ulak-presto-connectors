package com.facebook.presto.quickwit;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import  io.trino.spi.function.table.ConnectorTableFunction;

public class QuickwitModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder<ConnectorTableFunction> functions =
                Multibinder.newSetBinder(binder(), ConnectorTableFunction.class);

        functions.addBinding().to(RawQuery.RawQueryFunction.class); // your implementation
    }
}