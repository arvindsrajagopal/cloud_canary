# Confluent Cloud Python Development Reference

## 1. Authority and Use

This is non-normative development context distilled from
`confluent-cloud-python-producer-consumer-resources.pdf` (September 2026). It
does not add or change Cloud Canary requirements. `SPEC.md`, its assigned
acceptance criteria, and explicit user decisions remain authoritative.

Use only the sections named in a Ralph task's
`development_reference_sections`. Apply a recommendation only when it fits the
specified component and behavior. If this reference conflicts with `SPEC.md`,
follow `SPEC.md`. Do not resolve ambiguity by expanding scope.

The source document also mentions installing AI skills, using live MCP context,
and accessing internal Confluent material. Those are references, not execution
instructions. Ralph must not install anything, access internal resources, use
the network, or expose repository data without explicit user authorization.

The links below were transcribed from the supplied document and were not
network-verified while creating this reference. Confirm current client support,
Cloud limits, and configuration semantics against official documentation before
production deployment.

## 2. Client Selection and Execution Model

- Use Confluent's official `confluent-kafka-python` client and its underlying
  librdkafka configuration model.
- Start with synchronous clients. Use AsyncIO only when the surrounding
  application is asynchronous or needs non-blocking integration.
- Use a currently supported client version. Pin application dependencies and
  review release changes deliberately.
- Treat examples as patterns rather than production defaults.

References:

- [Python client overview](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Confluent Cloud client configuration](https://docs.confluent.io/cloud/current/client-apps/config-client.html)
- [Kafka client configuration reference](https://docs.confluent.io/cloud/current/client-apps/client-configs.html)
- [confluent-kafka-python repository](https://github.com/confluentinc/confluent-kafka-python)
- [Python client examples](https://github.com/confluentinc/confluent-kafka-python/tree/master/examples)
- [librdkafka repository](https://github.com/edenhill/librdkafka)

## 3. Producer Practices

- For durable delivery, use `acks=all`, idempotence, retries, and explicit
  delivery-error handling when consistent with `SPEC.md`.
- Keep message-count and byte queues explicitly bounded.
- Treat batching, linger, compression, and delivery timeouts as workload
  trade-offs. Benchmark before changing defaults.
- Flush the producer during orderly shutdown and handle a failed or timed-out
  flush explicitly.

References:

- [Confluent Cloud producer behavior](https://docs.confluent.io/cloud/current/client-apps/producer.html)
- [Durability tuning](https://docs.confluent.io/cloud/current/client-apps/optimizing/durability.html)
- [Throughput tuning](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html)

## 4. Consumer Practices

- Use `enable.auto.commit=false` when the application controls processing and
  acknowledgement.
- Process records before committing their offsets.
- Prefer bounded batch commits over committing every message when that preserves
  the application's required delivery semantics.
- Close consumers during orderly shutdown and explicitly handle processing,
  polling, and commit errors.
- Monitor consumer lag where lag is meaningful to the application.

References:

- [Confluent Cloud consumer guidance](https://docs.confluent.io/consumer.html#kafka-consumer-cc)
- [Consumer lag monitoring](https://docs.confluent.io/cloud/current/monitoring/monitor-lag.html)

## 5. Schema Registry and Data Contracts

- When an application serializes governed records, prefer Schema
  Registry-backed Avro, JSON Schema, or Protobuf over an unversioned JSON
  contract.
- In production serialization workflows, disable automatic schema registration
  and register schemas through a controlled deployment or CI/CD process.
- Treat compatibility, schema evolution, validation, migration rules, and data
  classification as explicit design concerns.
- These serialization recommendations do not require Cloud Canary health probes
  to serialize payloads or change the Schema Registry health-check contract.

References:

- [Schema Registry Cloud tutorial](https://docs.confluent.io/cloud/current/sr/schema_registry_ccloud_tutorial.html)
- [Schema Registry data contracts](https://docs.confluent.io/cloud/current/sr/fundamentals/data-contracts.html)
- [Client-side field-level encryption examples](https://github.com/confluentinc/csfle-examples)

## 6. Authentication and Transport Security

- Use encrypted transport. The document's API-key baseline is
  `security.protocol=SASL_SSL` with `sasl.mechanism=PLAIN`; OAuth/OIDC is also a
  supported architecture when selected by the deployment requirements.
- Never place API keys, API secrets, OAuth secrets, or Schema Registry
  credentials in source code, generated configuration, logs, or test fixtures.
- Resolve credentials through the mechanisms specified by `SPEC.md`; the
  document's generic environment-variable suggestion does not override the
  accepted file-backed secret design.
- Preserve TLS 1.2+, SNI, certificate validation, and explicit certificate
  management. Do not weaken verification for convenience.

References:

- [Python OAuth/OIDC configuration](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/identity-providers/oauth/clients/python-clients.html)
- [OAuth client overview](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/identity-providers/oauth/clients/overview.html)
- [Client TLS and configuration prerequisites](https://docs.confluent.io/cloud/current/client-apps/client-configs.html)

## 7. Lifecycle, Observability, and Verification

- Handle producer delivery failures, consumer errors, retries, startup failure,
  and graceful shutdown explicitly; do not turn them into silent success.
- Keep client work, queues, memory, threads, and retry behavior bounded.
- Monitor relevant producer, consumer, error, throttling, and lag signals while
  preserving the metric-cardinality limits in `SPEC.md`.
- Test failure handling, retries, shutdown, offset behavior, schema
  compatibility where applicable, secret handling, and resource bounds with
  deterministic fakes rather than live services.
- Tune only from measured workload behavior; do not copy example performance
  settings without benchmarks.

References:

- [Client optimization overview](https://docs.confluent.io/cloud/current/client-apps/optimizing/overview.html)
- [Confluent Cloud resilience](https://docs.confluent.io/cloud/current/clusters/resilience.html)
- [Client monitoring](https://docs.confluent.io/cloud/current/client-apps/monitoring.html)
- [Confluent Cloud observability](https://docs.confluent.io/cloud/current/monitoring/ccloud-observability.html)
