package nl.jgordijn.examples.groupby

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import reactor.core.publisher.Flux
import reactor.core.publisher.GroupedFlux
import reactor.core.publisher.SignalType
import reactor.core.scheduler.Schedulers
import java.util.logging.Level

typealias Key = Int
fun main() {
    val emitCounter = AtomicInteger()

    val countOccurrences = ConcurrentHashMap<Key, Long>()

    fun increment(group: Key) = countOccurrences.compute(group) { _, k -> (k ?: 0) + 1 }

    fun countNumbers(group: GroupedFlux<Key, Int>): Flux<Int> =
        group
//            .delayElements(Duration.ZERO)
            .log("countNumbers-${group.key()}", Level.INFO, SignalType.REQUEST, SignalType.ON_SUBSCRIBE, SignalType.ON_NEXT)
            .doOnNext { increment(group.key()) }

    Flux.generate<Int> { it.next(emitCounter.incrementAndGet()) }
        .log("groupBy", Level.INFO, SignalType.REQUEST, SignalType.ON_SUBSCRIBE, SignalType.ON_NEXT)
        .groupBy { it % 10 }
        .log("flatMap", Level.INFO, SignalType.REQUEST, SignalType.ON_SUBSCRIBE)
        .flatMap(::countNumbers, 9)
        .log("subscribe", Level.INFO, SignalType.REQUEST, SignalType.ON_SUBSCRIBE)
        .subscribeOn(Schedulers.parallel())
        .subscribe()

    everySecondDo {
        println("nrs emitted: ${emitCounter.get()} Occurrences per group: ${countOccurrences.toList().joinToString { (key, counts) -> "$key: $counts" }}")
    }

    Thread.sleep(10_000)
}

fun everySecondDo(fn: () -> Unit) {
    Flux.interval(Duration.ofSeconds(1))
        .doOnNext {
            fn()
        }
        .subscribeOn(Schedulers.parallel())
        .subscribe()
}
