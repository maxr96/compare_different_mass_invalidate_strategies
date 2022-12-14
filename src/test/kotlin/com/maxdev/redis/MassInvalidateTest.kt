package com.maxdev.redis

import io.quarkus.redis.datasource.RedisDataSource
import io.quarkus.test.junit.QuarkusTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import javax.inject.Inject
import kotlin.system.measureNanoTime
import kotlin.system.measureTimeMillis

@QuarkusTest
class MassInvalidateTest {

    @Inject
    lateinit var ds: RedisDataSource

    val randomValue = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been " +
            "the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and " +
            "scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into " +
            "electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release " +
            "of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like " +
            "Aldus PageMaker including versions of Lorem Ipsum."


    @AfterEach
    fun test(){
        ds.flushall()
    }
    @Test
    fun testNaiveApproach() {
        val stringCommands = ds.string(String::class.java)
        val timeToSetKeys = measureTimeMillis {
            for (i in 1..1000) {
                for (j in 1..2000) {
                    stringCommands.set("company_${i}_method_${j}", "$randomValue $j")
                }
            }
        }
        println("Time to fill key space: $timeToSetKeys")

        val keyCommands = ds.key()
        val timeToRemoveKeys = measureTimeMillis {
           val keys = keyCommands.keys("company_1000*")
           keyCommands.unlink(*keys.toTypedArray())
        }
        println("Time to find and remove keys: $timeToRemoveKeys")
        val timeToGetAndSetKeys = measureNanoTime {
            stringCommands.get("company_999_method_2010")
            stringCommands.set("company_999_method_2010", "$randomValue 2000")
        }
        println("Time to get and set keys: $timeToGetAndSetKeys")
        println(ds.execute("memory", "stats"))
    }

    @Test
    fun testHashApproach() {
        val hashCommands = ds.hash(String::class.java)
        val timeToSetKeys = measureTimeMillis {
            for (i in 1..1000) {
                for (j in 1..2000) {
                    hashCommands.hset("company_${i}", "method_${j}", "$randomValue $j")
                }
            }
        }
        println("Time to fill key space: $timeToSetKeys")
        val keyCommands = ds.key()
        val timeToRemoveKeys = measureTimeMillis {
            keyCommands.unlink("company_1000")
        }
        println("Time to find and remove keys: $timeToRemoveKeys")
        val timeToGetAndSetKeys = measureNanoTime {
            hashCommands.hget("company_999", "method_2010")
            hashCommands.hset("company_999", "method_2010", "$randomValue 2000")
        }
        println("Time to get and set keys: $timeToGetAndSetKeys")
        println(ds.execute("memory", "stats"))
    }

    @Test
    fun testSetApproach() {
        val stringCommands = ds.string(String::class.java)
        val setCommands = ds.set(String::class.java)
        val timeToSetKeys = measureTimeMillis {
            for (i in 1..1000) {
                for (j in 1..2000) {
                    stringCommands.set("method_${i}_${j}", "$randomValue $j")
                    setCommands.sadd("company_${i}", "method_${i}_${j}")
                }
            }
        }
        println("Time to fill key space: $timeToSetKeys")
        val keyCommands = ds.key()
        val timeToRemoveKeys = measureTimeMillis {
            val values = setCommands.smembers("company_1000")
            keyCommands.unlink(*values.toTypedArray())
        }
        println("Time to find and remove keys: $timeToRemoveKeys")
        val timeToGetAndSetKeys = measureNanoTime {
            stringCommands.get("method_1010_101")
            stringCommands.set("method_1010_101", "$randomValue 2000")
            setCommands.sadd("company_999", "method_1010_101")
        }
        println("Time to get and set keys: $timeToGetAndSetKeys")
        println(ds.execute("memory", "stats"))
    }
}

// Naive implementation
//Time to fill key space: 81.65 s
//Time to find and remove keys: 317 ms
//Time to get and set keys: 0.6 ms
//Total allocated memory: 1431.4 Mbytes
// {dataset.bytes: 1333860720, rss-overhead.bytes: 4620288, peak.percentage: 94.13092803955078, aof.buffer: 0, keys.bytes-per-key: 716, rss-overhead.ratio: 1.0031864643096924, allocator.allocated: 1433061344, clients.normal: 82064, fragmentation: 1.0151302814483643, lua.caches: 0, allocator-fragmentation.ratio: 1.0002214908599854, allocator.active: 1433378816, peak.allocated: 1520703048, total.allocated: 1431451856, allocator-fragmentation.bytes: 317472, replication.backlog: 0, dataset.percentage: 93.2352523803711, allocator-rss.ratio: 1.011593222618103, startup.allocated: 811816, overhead.total: 97591136, keys.count: 1998001, allocator.resident: 1449996288, db.0: {overhead.hashtable.main: 96697256, overhead.hashtable.expires: 0}, fragmentation.bytes: 21680704, clients.slaves: 0, allocator-rss.bytes: 16617472}

// Hash implementation
//Time to fill key space: 81.82 ms
//Time to find and remove keys: 1 ms
//Time to get and set keys: 0.47 ms
//Total allocated memory: 1376.5 Mbytes
// {dataset.bytes: 1375162232, rss-overhead.bytes: 4308992, peak.percentage: 90.48726654052734, aof.buffer: 0, keys.bytes-per-key: 1376607, rss-overhead.ratio: 1.0030839443206787, allocator.allocated: 1376260408, clients.normal: 20520, fragmentation: 1.0184054374694824, lua.caches: 0, allocator-fragmentation.ratio: 1.0031872987747192, allocator.active: 1380646912, peak.allocated: 1520703048, total.allocated: 1376042720, allocator-fragmentation.bytes: 4386504, replication.backlog: 0, dataset.percentage: 99.99501037597656, allocator-rss.ratio: 1.012012243270874, startup.allocated: 811816, overhead.total: 880488, keys.count: 999, allocator.resident: 1397231616, db.0: {overhead.hashtable.main: 48152, overhead.hashtable.expires: 0}, fragmentation.bytes: 25329792, clients.slaves: 0, allocator-rss.bytes: 16584704}

// Set implementation
//Time to fill key space: 170.32 ms
//Time to find and remove keys: 7 ms
//Time to get and set keys: 1,78 ms
//Total allocated memory:  1519.2 Mbytes
// {dataset.bytes: 1421655792, rss-overhead.bytes: 4022272, peak.percentage: 99.90282440185547, aof.buffer: 0, keys.bytes-per-key: 759, rss-overhead.ratio: 1.002610445022583, allocator.allocated: 1519889592, clients.normal: 20512, fragmentation: 1.0164660215377808, lua.caches: 0, allocator-fragmentation.ratio: 1.002873420715332, allocator.active: 1524256768, peak.allocated: 1520703048, total.allocated: 1519225376, allocator-fragmentation.bytes: 4367176, replication.backlog: 0, dataset.percentage: 93.62770080566406, allocator-rss.ratio: 1.0108832120895386, startup.allocated: 811816, overhead.total: 97569584, keys.count: 1999001, allocator.resident: 1540845568, db.0: {overhead.hashtable.main: 96737256, overhead.hashtable.expires: 0}, fragmentation.bytes: 25025608, clients.slaves: 0, allocator-rss.bytes: 16588800}