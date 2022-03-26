package nl.jgordijn.examples.limitrate

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.SignalType
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import kotlin.random.Random.Default.nextInt
import kotlin.random.Random.Default.nextLong

fun main() {

    val scheduler = Schedulers.parallel()

    val start = Instant.now()
    val job = Flux.create<Int> { sink ->
        sink.onRequest { demand ->
            scheduler.schedule({
                repeat(demand.toInt()) {
                    sink.next(nextInt())
                }
            }, 200, TimeUnit.MILLISECONDS)
        }
    }
        .log("demandflow", Level.INFO, SignalType.REQUEST)
        .limitRate(100)
        .flatMap({ nr ->
            Mono.fromCallable { nr.toString() }.delayElement(Duration.ofMillis(nextLong(10, 15)))
        }, 16)
        .subscribeOn(Schedulers.parallel())
        .take(1000)
        .doOnComplete {
            println("Time: ${Duration.between(start, Instant.now())}")
        }
        .subscribe()

    while (!job.isDisposed) {
        Thread.sleep(100)
    }
}
