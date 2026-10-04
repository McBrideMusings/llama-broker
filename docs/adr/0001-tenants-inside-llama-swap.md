# ADR-0001: Tenants are scheduled inside llama-swap

GPU tenancy is a `tenants:` section of the llama-swap config, and the process that loads models also decides which tenant may load next. No separate broker service sits in front of it. Tenant code lives in its own package, `internal/tenants`, and connects to the scheduler and the process stop path only through small hook points.
