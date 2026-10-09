## ADR-0007: Loyalty program as the most trusted source for personal data, without an anchor role

**Status:** Accepted

### Context
Survivorship needs a trust order between sources. Matching could also treat loyalty members as anchors to which all other records attach, which would simplify the graph.

### Decision
- Trust ranking is defined per attribute, not per source:
  - name, phone, date of birth: loyalty > webshop > marketplace; ties broken by recency;
  - email: same order, but a marketplace email never wins (relay or masked addresses);
  - address: loyalty (optionally billing); a webshop delivery address never becomes the customer address. It stays an attribute of the order (gifts, office deliveries).
- Loyalty members are not anchors in matching. All source records are equal nodes in the graph.
- A cluster without a loyalty member still gets a golden record, built from the best available sources.
- The trust ranking is versioned, like the matching rules.

### Consequences
- Matching covers customers who never joined the loyalty program, which is the hardest and most valuable part of the problem.
- Survivorship quality becomes measurable once the generator records each person's true current attributes (ADR-0008).

### Alternatives considered
- Loyalty as anchor: rejected; it would hide non-members and simplify away the core problem.

---
