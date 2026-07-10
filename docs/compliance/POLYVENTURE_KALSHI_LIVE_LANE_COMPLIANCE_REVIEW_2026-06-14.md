TL;DR: Polyventure is cleared for live lane submission. The updated Kalshi Member Agreement narrows — not expands — the restriction scope for geographic jurisdictions, and confirms CFTC federal oversight is unchanged. No new obligations are introduced that affect Polyventure's own-trading posture. Prior compliance anchor from 2026-05-08 holds.

## Scope and Sources

This report re-evaluates Polyventure's compliance posture ahead of live lane activation.

Sources reviewed:

- `assets/legal/KALSHI_MEMBER_AGREEMENT_UPDATED.md` — updated member agreement (v1.6, undated; retrieved 2026-06-14)
- Kalshi regulatory help page: https://help.kalshi.com/en/articles/13823765-how-is-kalshi-regulated (retrieved 2026-06-14)
- Prior review anchor: `docs/compliance/POLYVENTURE_KALSHI_MODE_PLAN_COMPLIANCE_REVIEW_2026-05-08.md`
- Original source artifact: `docs/compliance/source_docs/kalshi_member_agreement_pasted_2026-05-07.txt`

The developer agreement and shop terms are not re-reviewed here — no updated versions were provided. The prior review's developer agreement anchors remain operative unless a subsequent update supersedes them.

---

## Federal Regulatory Status

Kalshi is regulated by the **Commodity Futures Trading Commission (CFTC)** as a **Designated Contract Market (DCM)** under the Commodity Exchange Act (CEA). The CFTC is an independent federal agency established in 1974 and overseen by Congress.

This status is confirmed by the help page and is reiterated in the updated member agreement Section I ("U.S. Commodity Futures Trading Commission ('CFTC') designated contract market ('DCM')"). No change in regulatory designation has occurred. The platform operates under the CEA and all CFTC regulations, including Commission Regulations 22.2(e)(1) and 1.25 (member fund investment).

Member obligations continue to include compliance with "the CEA, CFTC regulations and all other applicable laws, rules, regulations, judgments, orders and rulings of any governmental authority" (Section VI). This is unchanged from the prior agreement.

The USA PATRIOT Act identity verification requirement (Section XIX) is unchanged.

**Federal regulatory posture: no change. CFTC DCM status confirmed.**

---

## Material Changes in the Updated Member Agreement

Both agreements are labeled v1.6. The version label is identical; the substantive changes are in Section VI and Section VII.B only. All other sections are textually identical.

### Change 1 — Section VI: Restricted Jurisdictions scope narrowed

**Prior agreement (2026-05-07):**

> You hereby further represent, warrant, and covenant to Kalshi that you are not domiciled in, organized in, or located in any jurisdiction in which **access to, use of, or trading on** the Platform is prohibited...you are **prohibited to access, use, or trade Contracts** on the Platform if you are domiciled in...any of the following jurisdictions...or any jurisdiction or territory that is the subject of **comprehensive** country-wide, territory-wide, or regional economic sanctions imposed by the United States.

**Updated agreement:**

> You hereby represent, warrant, and covenant to Kalshi that You are not domiciled in, organized in, or located in any jurisdiction in which **trading Event Contracts** on the Platform is prohibited...You are **prohibited from trading Event Contracts** on the Platform if You are domiciled in...any of the following jurisdictions...or any jurisdiction or territory that is the subject of country-wide, territory-wide, or regional economic sanctions imposed by the United States.

> **The restrictions set forth in this Section apply solely to the trading of Event Contracts and do not, in and of themselves, prohibit membership on, or non-trading access to, the Platform.**

> **Nothing in this Agreement shall prohibit a person or entity domiciled in, organized in, or located in a Restricted Jurisdiction from obtaining or maintaining membership on the Platform, accessing the Platform, or trading any contract or product other than an Event Contract...**

> **Notwithstanding anything to the contrary in this Agreement, Kalshi reserves the right, in its sole and absolute discretion, to grant, deny, condition, suspend, or revoke membership or access to the Platform to any person or entity, regardless of whether such person or entity is domiciled in, organized in, or located in a Restricted Jurisdiction.**

**Assessment:** The prior agreement prohibited access to, use of, OR trading on the Platform from Restricted Jurisdictions — a broad three-part prohibition. The updated agreement narrows this to trading Event Contracts only. Membership and non-trading platform access are now explicitly carved out as permitted. The word "comprehensive" before "country-wide" in the sanctions qualifier has also been removed, which may represent alignment with current OFAC sanctions language.

This is a **liberalization**: the restriction is narrower in scope in the updated agreement. Polyventure's operator is a US-based own-account trader, so this change is confirmatory, not restrictive.

The restricted jurisdiction list itself is **unchanged**.

### Change 2 — Section VII.B: Membership rights clarification

**Prior agreement (2026-05-07):**

> B. Your status as a Member may be limited, conditioned, restricted or terminated by Kalshi in accordance with the Kalshi Rulebook;

**Updated agreement:**

> B. Your status as a Member may be limited, conditioned, restricted or terminated by Kalshi in accordance with the Kalshi Rulebook. **For the avoidance of doubt, the foregoing includes the right to grant or continue membership to any person or entity domiciled in, organized in, or located in a Restricted Jurisdiction (as defined in Section VI), subject to such terms, conditions, and limitations as Kalshi may impose;**

**Assessment:** This is a housekeeping addition consistent with the Section VI change — it clarifies that Kalshi's membership discretion extends to Restricted Jurisdiction persons. No new obligations on the operator. No Polyventure impact.

---

## Polyventure Alignment Analysis

The prior compliance review established five forward implementation postures. Each is re-evaluated here against the updated agreement.

| Posture | Prior status | Updated status |
|---|---|---|
| Own-trading purpose is primary | Aligned | Aligned — unchanged |
| Benchmarking posture excluded | Aligned | Aligned — unchanged |
| Redistribution posture excluded | Aligned | Aligned — unchanged |
| Retention is bounded | Aligned | Aligned — unchanged |
| Third-party trading facilitation excluded | Aligned | Aligned — unchanged |

The updated agreement introduces no new data retention restrictions, no new API use constraints, no new automation prohibitions, and no new reporting obligations. The developer agreement constraints (own-trading API use, no caching/aggregation outside own-trading purpose, no service benchmarking, no third-party trading facilitation) were not revised in the materials provided and remain operative as documented in the 2026-05-08 review.

The only operative changes are the Section VI/VII.B jurisdictional liberalizations, which have no bearing on a US-based own-account operator.

---

## Live Lane Requirements Review

Live lane activation requires use of live API credentials scoped to the operator's own account. The following member agreement obligations apply in live mode and are confirmed met by Polyventure's current implementation posture:

- **Section V.E** — no unidentified person may access the Services. Polyventure is a single-operator shell; multi-user access is excluded by design.
- **Section VI** — operator must comply with CEA, CFTC regulations, and Applicable Law. The operator (US-domiciled) is not in a Restricted Jurisdiction. No statutory disqualification applies.
- **Section VII.R** — operator is solely responsible for system security not less than industry standard. Polyventure uses environment-variable credential management, `.gitignore`d secrets, and no hardcoded keys. This obligation is met.
- **Section VII.K/L** — operator must provide identity information on request and authorizes Kalshi background investigation. This is an operator personal obligation; Polyventure as a system has no bearing on it.
- **Section XII** — transaction price/quantity data and order data are licensed to Kalshi perpetually. Polyventure does not purport to restrict Kalshi's use of this data. Retention of this data within Polyventure is for own-trading facilitation only, which is consistent with the agreement.

No live lane blocker identified.

---

## Conclusion

**Decision: go — Polyventure cleared for live lane submission.**

The CFTC DCM federal regulatory status is confirmed and unchanged. The updated member agreement is a narrower instrument than the prior version: restrictions apply only to trading Event Contracts in Restricted Jurisdictions, not to platform access or membership generally. No new obligations affect Polyventure's own-trading posture, automation architecture, credential management, or retention model.

The 2026-05-08 compliance anchor remains valid and is reinforced by this review.

The developer agreement (API use constraints) was not revised in the materials provided. If an updated developer agreement is published by Kalshi, a targeted re-review of the API use, data caching, and benchmarking constraints is recommended before that update is treated as operative.

## Next-actions

- Proceed to live lane activation sequence
- If Kalshi publishes an updated Developer Agreement, re-review the API data use and own-trading constraint sections against Polyventure's retention and analytics posture
- Retain this report as the live lane compliance clearance artifact
