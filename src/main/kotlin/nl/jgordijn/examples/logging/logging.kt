package nl.jgordijn.examples.logging

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.SignalType
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level
import kotlin.random.Random.Default.nextLong

fun main() {
    val counter = AtomicLong()

    fun process(nr: Long): Mono<Long> =
        Mono.just(nr).delayElement(Duration.ofMillis(nextLong(1, 25)))

    val x = Flux.generate<Long> { it.next(counter.incrementAndGet()) }
        .log("beforeFlatmap", Level.INFO, SignalType.REQUEST)
        .flatMap(::process)
        .log("beforeTake", Level.INFO, SignalType.REQUEST)
        .take(100)
        .log("beforeSubscribe", Level.INFO, SignalType.REQUEST)
        .subscribeOn(Schedulers.parallel())
        .subscribe()

    while(!x.isDisposed)
        Thread.sleep(100)
    println("Counter: ${counter.get()}")
}

// 99
//

