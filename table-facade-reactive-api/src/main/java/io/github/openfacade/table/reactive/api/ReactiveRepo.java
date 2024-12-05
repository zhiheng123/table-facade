package io.github.openfacade.table.reactive.api;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveRepo<T> {
    Flux<T> findAll();

    Mono<Long> count();
}
