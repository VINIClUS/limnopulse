# ADR-008 — Customer/vendor owns physical hardware accuracy and local installation.

**Status:** Accepted

## Context

LimnoPulse receives readings from customer-owned equipment it does not manufacture, install, calibrate, warrant, or physically inspect. Software quality signals cannot certify sensor accuracy.

## Decision

Customers and hardware vendors retain responsibility for physical selection, installation, calibration execution, maintenance, and accuracy claims. LimnoPulse records provenance, calibration metadata, data quality, and operational health without claiming certification.

## Consequences

The platform can expose uncertainty honestly and remain hardware-independent. Product and support material must distinguish observed data quality from physical accuracy and cannot imply installation services.

## V4 traceability

V4 §§4, 11, 24 Phase 6, 27, and 31 preserve this responsibility boundary and flag overstated confidence as a risk.

## Implementation gate

Phase 6 must distinguish unknown from overdue calibration, expose provenance, and prevent missing data from appearing healthy. Phase 9 connector contracts must retain vendor responsibility.

## Non-goals

This record does not certify sensors, guarantee readings, finance hardware, or make LimnoPulse the installer or warrantor.
