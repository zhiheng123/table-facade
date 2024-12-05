package io.github.openfacade.table.api;

import java.util.List;

public interface Repo<T> {
    List<T> findAll();

    long count();
}
