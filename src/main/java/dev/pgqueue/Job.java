package dev.pgqueue;

import java.time.Instant;

public record Job(long id, byte[] payload, Instant createdAt) {}
