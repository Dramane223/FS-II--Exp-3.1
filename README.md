# Experiment 3.1.1: Reliability in Event-Driven Systems (Kafka)

**Name:** Dramane Traore
**UID:** 24BCY70270

## Aim
Implement retry with exponential backoff, a Dead Letter Queue (DLQ), and idempotent processing in a Kafka-based event-driven system.

## Tech Stack
Java 17, Spring Boot 3.3, Spring Kafka, Apache Kafka (Docker, KRaft), Maven

## Architecture
REST API -> Producer -> `scheduled-posts` topic -> Consumer
Consumer failure -> retries (1s, 2s, 4s) -> `scheduled-posts-dlq` -> DLQ consumer

## Reliability Mechanisms
- **Retry:** `DefaultErrorHandler` with `ExponentialBackOff` (1s, 2s, 4s)
- **DLQ:** `DeadLetterPublishingRecoverer` publishes exhausted messages to `scheduled-posts-dlq`
- **Idempotency:** `IdempotencyService` tracks processed `eventId`s; duplicates are skipped

## How to Run
```bash
docker compose up -d
mvn spring-boot:run
```

## API
- `POST /api/posts` body: `{"eventId":"...","content":"...","failureMode":"none|transient|permanent"}`
- `GET /api/status` shows processed events and dead letters

## Test Scenarios
| Scenario | failureMode | Expected |
|---|---|---|
| Success | none | Processed once |
| Retry | transient | Fails twice, succeeds on attempt 3 |
| DLQ | permanent | Fails 4 times, moved to DLQ |
| Duplicate | none (same eventId) | Skipped by idempotency check |

## Outcome
Reliable message processing, failed messages isolated in the DLQ, and duplicate processing prevented.